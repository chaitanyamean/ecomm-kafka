# Order Saga — a Kafka Choreography Microservices Project

A learning project implementing a classic **Saga pattern** across three Spring Boot
microservices connected by Kafka, with Postgres as the per-service datastore.

This README is both reference documentation and a journal of how the project was
built, including the problems hit along the way.

---

## 1. What we built

Three independent Spring Boot services that collaborate to process an order
without ever calling each other directly. They talk only through Kafka topics.

| Service              | Port | DB            | Role                                              |
| -------------------- | ---- | ------------- | ------------------------------------------------- |
| **order-service**    | 8080 | `orders`      | Accepts HTTP, persists orders, orchestrates saga  |
| **inventory-service**| 8081 | `inventory`   | Reserves and releases stock                       |
| **payment-service**  | 8082 | `payment_db`  | Charges the customer, decides outcome             |

Infrastructure (Docker containers):

| Component  | Port    | Container name |
| ---------- | ------- | -------------- |
| Postgres   | `5433`  | `pg`           |
| Kafka      | `9092`  | `kafka`        |

---

## 2. Architecture

```
                     POST /orders
                           │
                           ▼
             ┌────────────────────────┐
             │     order-service      │
             │     (port 8080)        │
             │                        │
             │  ┌──────────────────┐  │
             │  │ orders DB        │  │
             │  └──────────────────┘  │
             └───┬────────────▲───────┘
                 │            │
     publishes   │            │   consumes
    order-created│            │   inventory-failed
                 │            │   payment-processed
                 │            │   payment-failed
                 ▼            │
         ┌──────────────────────────┐
         │     Kafka (9092)         │
         │   topic log on disk      │
         └────┬────────────┬────────┘
              │            │
     consumes │            │  publishes
   order-     │            │   inventory-reserved
   created    │            │   inventory-failed
              ▼            │   inventory-released
    ┌─────────────────────┐│
    │ inventory-service   ││
    │ (port 8081)         ││
    │ ┌───────────────┐   ││
    │ │ inventory DB  │   │▼
    │ └───────────────┘   │ ┌─────────────────────┐
    │  listens:           │ │  payment-service    │
    │   order-created     │ │  (port 8082)        │
    │   payment-failed    │ │                     │
    │  publishes:         │ │  ┌──────────────┐   │
    │   inventory-reserved│ │  │ payment DB   │   │
    │   inventory-failed  │ │  └──────────────┘   │
    │   inventory-released│ │  listens:           │
    └─────────────────────┘ │   inventory-reserved│
                            │  publishes:         │
                            │   payment-processed │
                            │   payment-failed    │
                            └─────────────────────┘
```

**Why "choreography" saga:** no central coordinator. Each service reacts to the
events it cares about and emits its own events. The three services together
form the state machine.

---

## 3. The three paths through the saga

### Path A — happy path
```
POST /orders ─► order:PENDING ─► order-created ─► inventory reserved
                                              ─► inventory-reserved
                                              ─► payment ok
                                              ─► payment-processed
                                              ─► order:COMPLETED
```

### Path B — out of stock
```
POST /orders ─► order:PENDING ─► order-created ─► inventory NOT enough
                                              ─► inventory-failed
                                              ─► order:CANCELLED
                                                 (no payment attempt)
```

### Path C — payment declined (compensation)
```
POST /orders ─► order:PENDING ─► order-created ─► inventory reserved
                                              ─► inventory-reserved
                                              ─► payment fails
                                              ─► payment-failed
                                              ─► order:CANCELLED
                                              ─► inventory-released
                                                 (stock added back)
```

Payment success/failure in this demo is decided by a simple rule in
`PaymentService`:
```java
boolean paymentOk = !order.getProductName().toLowerCase().contains("fail");
```
So you can trigger Path C by including `FAIL` in the product name.

---

## 4. The code

### order-service (`com.example.Kafka.demo`)
- `entity/Order.java` – JPA entity (`orders` table)
- `repository/OrderRepository.java`
- `controller/OrderController.java` – `POST /orders`
- `service/OrderService.java` – saves order with status `PENDING`, triggers producer
- `service/OrderProducer.java` – publishes `order-created`
- `service/OrderConsumer.java` – listens on `payment-processed`, `payment-failed`,
  `inventory-failed`

### inventory-service (`com.example.inventory_service`)
- `entity/Inventory.java` – JPA entity (`inventory` table)
- `dto/Order.java` – local DTO for incoming event payloads
- `repository/InventoryRepository.java`
- `service/InventoryService.java` – listens on `order-created` and `payment-failed`,
  publishes `inventory-reserved`, `inventory-failed`, `inventory-released`

### payment-service (`com.example.payment_service`)
- `entity/Payment.java` – JPA entity (`payments` table)
- `dto/Order.java` – local DTO
- `repository/PaymentRepository.java`
- `service/PaymentService.java` – listens on `inventory-reserved`, publishes
  `payment-processed` or `payment-failed`

### A note about DTOs

Each service has **its own local `Order` class**. There is no shared library.
Jackson deserializes JSON by matching field names, so the two Order classes
don't need to be the same Java type — they just need the same field names.
This keeps the services decoupled.

### Kafka serialization pattern used everywhere

Every service uses the same configuration:

```yaml
spring:
  kafka:
    consumer:
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        spring.json.value.default.type: com.example.<service>.dto.Order
    producer:
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      properties:
        spring.json.add.type.headers: false
```

- Producer does not write `__TypeId__` header → messages are portable across services
- Consumer uses `value.default.type` to know what to deserialize to

---

## 5. The build journey, in order

Rough chronological account of how the project came together.

### Stage 1 — bare skeletons
Three Spring Initializr projects existed. order-service had a tiny
string-echo Kafka flow (`GET /order/{event}`). inventory-service and
payment-service were empty.

### Stage 2 — flesh out order-service as the HTTP front door
- Added `Order` entity, `OrderRepository`, `OrderService`
- Replaced the string flow with a real one: `POST /orders` → persist
  `PENDING` → publish `order-created`
- First end-to-end test: HTTP 200, row in Postgres, JSON message on Kafka

### Stage 3 — switch Kafka to JSON serialization (trainer's config)
- Added `JsonSerializer`/`JsonDeserializer` with `spring.json.value.default.type`
  pointing to the Order DTO
- This broke the earlier String flow; cleaned up by deleting the leftover
  `sendOrderEvent(String)` and `consume(String)` methods

### Stage 4 — build inventory-service
- Local `dto/Order.java` for the inbound event
- `Inventory` entity + repository
- `InventoryService` with `@KafkaListener("order-created")`
- Reserved stock if available → publish `inventory-reserved`
- Otherwise → publish `inventory-failed`
- Added `releaseInventory` compensation listener on `payment-failed`

### Stage 5 — build payment-service
- Local `dto/Order.java`
- `Payment` entity + repository
- `PaymentService` with `@KafkaListener("inventory-reserved")`
- Decide outcome via productName rule
- Publish `payment-processed` or `payment-failed`

### Stage 6 — close the loop in order-service
- Added `handleInventoryFailure` listener on `inventory-failed`
- Now every order reaches a terminal status (`COMPLETED` or `CANCELLED`),
  nothing stays `PENDING`

### Stage 7 — verified all three paths end to end
See section 3.

---

## 6. Problems we solved along the way

Keep this section handy — these will probably bite again.

### 6.1 `main.java.*` auto-imports from the IDE

The Java Language Server didn't recognize `src/main/java` as the source root, so
when the IDE auto-imported a class it produced paths like
`import main.java.com.example.Kafka.demo.entity.Order;` which don't compile.

**Fix:** `Cmd+Shift+P` → *Java: Clean Java Language Server Workspace* →
*Restart and delete*. Then reopen the correct subproject folder so the Maven
project gets imported properly.

### 6.2 Missing `package` / `import` statements

Several times, newly created files were missing their `package` statement and
all their imports. Maven caught them immediately with
`cannot find symbol: class Order`, `cannot find symbol: class Service`, etc.

**Fix:** always start a Java file with
```java
package <...>;

import <...>;
```

### 6.3 Jakarta vs javax (Spring Boot 3+/4)

Boot 3 and 4 use the `jakarta.persistence.*` namespace, not `javax.persistence.*`.
`@Entity`, `@Id`, `@GeneratedValue`, `@Table` all come from `jakarta.persistence`.

### 6.4 Spring Boot 4 Jackson 3 vs Spring Kafka Jackson 2 mismatch

payment-service failed to start with
`java.lang.NoClassDefFoundError: com/fasterxml/jackson/core/type/TypeReference`.

Spring Boot 4.1.1 moved its managed Jackson artifacts to `tools.jackson.core:*`
(Jackson 3), but Spring Kafka's `JsonSerializer`/`JsonDeserializer` still
references classes in the old `com.fasterxml.jackson.core` package.

**Fix:** add explicit Jackson 2 to the pom:
```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.22.3</version>
    <scope>compile</scope>
</dependency>
```

(inventory-service already had this. payment-service didn't, and failed.)

### 6.5 `ClassFormatError: Invalid method Code length 0`

Hit this twice — once for `Inventory`, once for `Payment`. Looks terrifying
but is almost always a stale/corrupt `.class` left over in `target/` from an
earlier incomplete compile.

**Fix:**
```bash
./mvnw clean compile
./mvnw spring-boot:run
```

Any time you see `ClassFormatError`, `NoClassDefFoundError`, or `NoSuchMethodError`
on your own classes, do `./mvnw clean` first.

### 6.6 Port 5432 conflict with native Postgres

Running Docker Postgres on 5432 failed because macOS already had a native
Postgres listening there.

**Fix:** mapped the Docker container to a different host port (`-p 5433:5432`).
Application configs point at port 5433.

### 6.7 Postgres GSSAPI / Kerberos realm error

```
could not initiate GSSAPI security context: Minor code may provide more information:
Cannot find KDC for realm "KERBEROS.MICROSOFTONLINE.COM"
```

libpq attempts GSSAPI first on an Entra/AD-joined Mac. Local Postgres doesn't
speak Kerberos, so the first attempt fails before password auth is tried.

**Fix:** append `?gssEncMode=disable` to the JDBC URL, or set
`PGGSSENCMODE=disable` for psql.

```yaml
url: jdbc:postgresql://localhost:5433/orders?gssEncMode=disable
```

### 6.8 Docker container name collision

Trying to re-run `docker run --name pg ...` after a failed earlier run gave
`Conflict. The container name "/pg" is already in use`.

**Fix:**
```bash
docker rm -f pg
```
then re-run.

### 6.9 Topic naming consistency

inventory-service initially published `quantity-not-available`, but order-service's
listener (later added) expected `inventory-failed`. Choreography breaks silently
when producers and consumers disagree on a topic name.

**Fix:** picked one name (`inventory-failed`) and used it on both sides. Also
matched the parallel of `payment-failed`.

### 6.10 Code changes don't apply without JVM restart

The biggest conceptual gotcha. Added `handleInventoryFailure` to
`OrderConsumer.java`, recompiled, but orders kept getting stuck in `PENDING`.

**Why:** Spring registers `@KafkaListener` methods **once at startup**. The
running JVM has the old method set compiled into memory. Editing the source
file and recompiling only changes the on-disk `.class` file; the running
process keeps running old code.

**Fix:** every time you add/change `@KafkaListener`, `@Bean`, `@Scheduled`,
`@EventListener`, you must restart that service. There is no magic hot-reload.

### 6.11 Order status "stuck" in PENDING across restarts

Related to above. When order #9 was POSTed, order-service was running without
the new listener, so even though inventory-service eventually sent
`inventory-failed`, nobody consumed it.

**But** — because Kafka persists messages durably to disk, after we restarted
order-service with the new listener running, inventory-service's old
`inventory-failed` message got consumed and order #9 was flipped to
`CANCELLED` retroactively. See Section 7 for how Kafka pulls that off.

---

## 7. Key concepts learned

### Why Kafka "remembers" messages while services are down

Kafka is not a traditional in-memory queue. Each topic is a **sequence of
append-only `.log` files on disk**. When a producer sends, Kafka appends bytes
to the current segment file and (optionally) fsyncs to disk. Messages are
never removed on consumption; they are removed only when they age out of the
retention window (default **7 days**).

Where the files actually live inside the Kafka container:
```
/tmp/kraft-combined-logs/order-created-0/00000000000000000000.log
```

You can inspect them with:
```bash
docker exec kafka /opt/kafka/bin/kafka-dump-log.sh \
  --files /tmp/kraft-combined-logs/order-created-0/00000000000000000000.log \
  --print-data-log
```

### How a consumer picks up where it left off

Each **consumer group** has its own progress tracker: a 64-bit **offset** per
partition. The broker keeps per-group offsets in a special internal topic called
`__consumer_offsets`. When a consumer processes a message, it commits the new
offset back to Kafka. Next time the service starts, it reads from the committed
offset forward.

Check progress:
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group inventory-group
```

Columns to watch:
- `CURRENT-OFFSET` — how far this group has read
- `LOG-END-OFFSET` — how many messages exist on the topic
- `LAG` — unread backlog. Should be 0 when everything is caught up.

### `auto-offset-reset`

Setting in each service's `application.yaml`. Controls what happens when a
consumer group has **no committed offset** at all (first start, or offsets
expired):

- `earliest` — start from the very first message (what we used — good for demos)
- `latest` — start only from new messages that arrive after you connect

### Decoupling via Kafka

Because the only thing linking the services is a Kafka broker and a topic
name string, any of them can be redeployed, moved to a different VPS, or
replaced with a service written in a different language. Nothing changes
in the others.

The connection recipe between any two services is:
1. Both point at the same `bootstrap-servers`
2. They agree on a topic name
3. They agree on the JSON payload shape

---

## 8. How to run the whole stack locally

### 8.1 Infrastructure (Docker)

**Postgres:**
```bash
docker run --name pg \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=orders -p 5433:5432 -d postgres:16
```

Then create the other two databases:
```bash
docker exec pg psql -U postgres -c "CREATE DATABASE inventory;"
docker exec pg psql -U postgres -c "CREATE DATABASE payment_db;"
```

**Kafka (single-node, KRaft mode):**
```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  bitnami/kafka:3.7
```

### 8.2 Seed inventory

Hibernate auto-creates the `inventory` table on first run; after inventory-service
has started once, seed some rows:
```bash
docker exec pg psql -U postgres -d inventory -c \
  "INSERT INTO inventory (product_id, product_name, quantity) VALUES
   (1,'Wireless Mouse',10),
   (2,'Mechanical Keyboard',5),
   (3,'USB-C Hub',8);"
```

### 8.3 Start the services

Three terminals, one per service:
```bash
cd order-service     && ./mvnw spring-boot:run
cd inventory-service && ./mvnw spring-boot:run
cd payment-service   && ./mvnw spring-boot:run
```

### 8.4 Test each path

**Happy path:**
```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"productId":3,"productName":"USB-C Hub","quantity":1}'
```
Expect: order → `COMPLETED`, inventory `3` decremented by 1, payments row `PROCESSED`.

**Out of stock:**
```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"productId":2,"productName":"Mechanical Keyboard","quantity":999}'
```
Expect: order → `CANCELLED`, inventory unchanged.

**Payment declined:**
```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"productId":1,"productName":"FAIL Me","quantity":1}'
```
Expect: order → `CANCELLED`, inventory reserved then released, payments row `FAILED`.

**Inspect:**
```bash
docker exec pg psql -U postgres -d orders     -c "SELECT * FROM orders ORDER BY id;"
docker exec pg psql -U postgres -d inventory  -c "SELECT * FROM inventory;"
docker exec pg psql -U postgres -d payment_db -c "SELECT * FROM payments;"
```

---

## 9. Running everything with Docker Compose

A `docker-compose.yml` at the project root boots the entire stack —
Postgres + Kafka + all three services — with one command.

### 9.1 File layout

```
microservice/
├── docker-compose.yml
├── infra/
│   └── init-db.sql            # pre-creates orders / inventory / payment_db
├── order-service/
│   ├── Dockerfile             # multi-stage: temurin 25 JDK -> JRE
│   └── .dockerignore
├── inventory-service/
│   ├── Dockerfile
│   └── .dockerignore
└── payment-service/
    ├── Dockerfile
    └── .dockerignore
```

Each service's Dockerfile is a multi-stage build: a JDK image compiles the
fat jar with `./mvnw package`, then only the jar is copied into a slim JRE
image for runtime.

### 9.2 Boot the stack

```bash
docker compose up -d --build
```

What that does:
1. Builds three service images from their Dockerfiles
2. Starts Postgres and runs `infra/init-db.sql` on first boot to create
   the three databases
3. Starts Kafka in KRaft mode with a dual listener:
   - `kafka:9092` — internal, used by services inside the compose network
   - `localhost:9094` — external, available on the host for CLI tooling
4. Waits for Postgres and Kafka to be healthy before starting the apps

### 9.3 Seed inventory and test

```bash
docker exec order-saga-postgres psql -U postgres -d inventory -c \
  "INSERT INTO inventory (product_id, product_name, quantity) VALUES
   (1, 'Wireless Mouse', 10),
   (2, 'Mechanical Keyboard', 5),
   (3, 'USB-C Hub', 8);"

# Happy path
curl -X POST http://localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"productId":3,"productName":"USB-C Hub","quantity":2}'

# Fail path (payment declined)
curl -X POST http://localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"productId":1,"productName":"FAIL Me","quantity":1}'
```

Verify:
```bash
docker exec order-saga-postgres psql -U postgres -d orders     -c "SELECT * FROM orders;"
docker exec order-saga-postgres psql -U postgres -d inventory  -c "SELECT * FROM inventory;"
docker exec order-saga-postgres psql -U postgres -d payment_db -c "SELECT * FROM payments;"
```

### 9.4 Common operations

```bash
# View logs for one service
docker compose logs -f order-service

# Rebuild one service after code change
docker compose up -d --build order-service

# Tear down (keep volumes)
docker compose down

# Tear down and delete volumes (fresh DB next time)
docker compose down -v
```

### 9.5 Problems hit while writing the compose file

- **Bitnami Kafka image tag `3.7` no longer exists.** Bitnami rotates tags
  often. Switched to `apache/kafka:latest`, which uses `KAFKA_*` env vars
  without the `_CFG_` prefix that bitnami uses (`KAFKA_NODE_ID` instead of
  `KAFKA_CFG_NODE_ID`).
- **Replication factor on a single-broker Kafka.** `apache/kafka:latest`
  defaults internal topics (`__consumer_offsets`, `__transaction_state`,
  group-coordinator and share-coordinator state) to replication factor 3.
  With only one broker, those topics couldn't be created, so no consumer
  group could form, no listener received messages, orders stayed PENDING
  forever. Fix: override all these to 1 in the compose env block:
  ```yaml
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
  KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_GROUP_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
  ```
- **The `advertised.listeners` gotcha.** Services inside the compose
  network reach Kafka at `kafka:9092`. The host reaches it at
  `localhost:9094`. If `advertised.listeners` says only `localhost:9092`,
  services inside the network can't resolve `localhost` to Kafka and the
  connection fails silently. The dual-listener config fixes this.

---

## 10. What's not done yet

- **Proper atomicity in inventory reservation.** Currently a race between two
  concurrent orders for the same product can oversell. Fix: optimistic locking
  via a `@Version` field on `Inventory`, or an atomic SQL update.
- **Resilient error handling.** A listener that throws will keep retrying the
  same message forever. Need dead-letter topics and bounded retries.
- **`productId` is `@GeneratedValue(IDENTITY)` on `Inventory`.** Should be a
  natural key (not auto-generated), with a separate surrogate `id` if desired.
- **Observability.** No metrics, no distributed tracing. Would want
  Micrometer + Prometheus + Zipkin/OpenTelemetry to actually trace a single
  order across the three services.
- **Security.** Kafka and Postgres are open on localhost; in production both
  should require auth and TLS.

---

## 11. Mental model to carry forward

- Each microservice owns its own data and its own copy of the message schema.
  There is no shared code between them.
- Kafka is the only communication substrate. Services never call each other
  directly.
- The "contract" between services is a **topic name** and a **JSON shape**.
  Both can evolve as long as both sides agree.
- Kafka messages are durable on disk. Services can be down and the system
  catches up when they come back.
- Code changes to listeners don't apply until the service restarts.
- Choreography saga: no coordinator, each service reacts and emits.
  Simpler for a few services; harder to monitor as the system grows.

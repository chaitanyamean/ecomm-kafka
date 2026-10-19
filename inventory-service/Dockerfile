# ---- Build stage ----
FROM eclipse-temurin:25-jdk AS build
WORKDIR /app

COPY .mvn ./.mvn
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw -q -B dependency:go-offline || true

COPY src ./src
RUN ./mvnw -q -B -DskipTests package

# ---- Runtime stage ----
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

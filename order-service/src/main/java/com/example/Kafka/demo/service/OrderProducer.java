package com.example.Kafka.demo.service;

import com.example.Kafka.demo.entity.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String sendOrderObjectEvent(Order order) {
        System.out.println("Sending order event: " + order.getId());
        kafkaTemplate.send("order-created", order);
        return "Order Object sent to Kafka: " + order.getId();
    }
}

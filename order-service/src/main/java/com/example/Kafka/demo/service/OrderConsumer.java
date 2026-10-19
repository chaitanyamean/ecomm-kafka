package com.example.Kafka.demo.service;

import com.example.Kafka.demo.entity.Order;
import com.example.Kafka.demo.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @Autowired
    private OrderRepository repo;

    @KafkaListener(topics = "payment-processed", groupId = "order-group")
    public void handlePaymentSuccess(Order order) {
        Order existing = repo.findById(order.getId())
                .orElseThrow(() -> new IllegalStateException("Order not found: " + order.getId()));
        existing.setStatus("COMPLETED");
        repo.save(existing);
    }

    @KafkaListener(topics = "payment-failed", groupId = "order-group")
    public void handlePaymentFailure(Order order) {
        System.out.println("Handling payment-failed");
        Order existing = repo.findById(order.getId())
                .orElseThrow(() -> new IllegalStateException("Order not found: " + order.getId()));
        existing.setStatus("CANCELLED");
        repo.save(existing);
    }

    @KafkaListener(topics = "inventory-failed", groupId="order-group")
    public void handleInventoryFailure(Order order) {
        System.out.println("Handling inventory failed");
        Order existing = repo.findById(order.getId())
                            .orElseThrow(() -> new IllegalStateException("Order not found: " + order.getId()));
        existing.setStatus("CANCELLED");
        repo.save(existing);
    }
}

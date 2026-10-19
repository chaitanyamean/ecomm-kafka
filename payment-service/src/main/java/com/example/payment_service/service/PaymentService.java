package com.example.payment_service.service;

import com.example.payment_service.dto.Order;
import com.example.payment_service.entity.Payment;
import com.example.payment_service.repository.PaymentRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

    private final PaymentRepository repo;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentService(PaymentRepository repo, KafkaTemplate<String, Object> kafkaTemplate) {
        this.repo = repo;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "inventory-reserved", groupId = "payment-group")
    public void processPayment(Order order) {
        System.out.println("Processing payment for order: " + order.getId()
                + ", productName=" + order.getProductName());

        // Simple demo rule: fail if productName contains "FAIL" (case-insensitive).
        // Anything else succeeds. Lets us trigger either path from a POST payload.
        String name = order.getProductName() == null ? "" : order.getProductName().toLowerCase();
        boolean paymentOk = !name.contains("fail");

        Payment payment = new Payment();
        payment.setOrderId(order.getId());
        payment.setStatus(paymentOk ? "PROCESSED" : "FAILED");
        repo.save(payment);

        if (paymentOk) {
            kafkaTemplate.send("payment-processed", order);
            System.out.println("Payment processed for order: " + order.getId());
        } else {
            kafkaTemplate.send("payment-failed", order);
            System.out.println("Payment failed for order: " + order.getId());
        }
    }

}

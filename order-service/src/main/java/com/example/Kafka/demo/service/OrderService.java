package com.example.Kafka.demo.service;

import com.example.Kafka.demo.entity.Order;
import com.example.Kafka.demo.repository.OrderRepository;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderProducer orderProducer;

    public OrderService(OrderRepository orderRepository, OrderProducer orderProducer) {
        this.orderRepository = orderRepository;
        this.orderProducer = orderProducer;
    }

    public String createOrder(Order order) {
        order.setStatus("PENDING");
        orderRepository.save(order);
        orderProducer.sendOrderObjectEvent(order);
        return "Order created and saga started";
    }

}

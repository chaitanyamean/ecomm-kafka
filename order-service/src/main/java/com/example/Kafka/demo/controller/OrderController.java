package com.example.Kafka.demo.controller;

import com.example.Kafka.demo.entity.Order;
import com.example.Kafka.demo.service.OrderService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/orders")
    public String createOrder(@RequestBody Order order) {
        return orderService.createOrder(order);
    }

}

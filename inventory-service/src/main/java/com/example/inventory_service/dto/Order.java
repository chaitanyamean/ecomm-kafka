package com.example.inventory_service.dto;

import lombok.Data;

/**
 * Local representation of the Order event received on the "order-created" topic.
 * This is a DTO, not a persisted entity — it mirrors the JSON shape produced by order-service.
 */
@Data
public class Order {
    private Long id;
    private long productId;
    private String productName;
    private int quantity;
    private String status;
}

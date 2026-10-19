package com.example.inventory_service.service;

import com.example.inventory_service.dto.Order;
import com.example.inventory_service.entity.Inventory;
import com.example.inventory_service.repository.InventoryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class InventoryService {

    @Autowired
    private InventoryRepository repo;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "order-created", groupId = "inventory-group")
    public void reserveInventory(Order order) {
        System.out.println("Received order-created for order: " + order.getId()
                + ", productId=" + order.getProductId()
                + ", quantity=" + order.getQuantity());

        Optional<Inventory> inventoryOpt = repo.findById(order.getProductId());

        if (inventoryOpt.isEmpty() || inventoryOpt.get().getQuantity() < order.getQuantity()) {
            System.out.println("Inventory not available for order: " + order.getId());
            kafkaTemplate.send("inventory-failed", order);
            return;
        }

        Inventory inventory = inventoryOpt.get();
        inventory.setQuantity(inventory.getQuantity() - order.getQuantity());
        repo.save(inventory);
        kafkaTemplate.send("inventory-reserved", order);
        System.out.println("Inventory reserved for order: " + order.getId());
    }

    // Compensation method
@KafkaListener(topics = "payment-failed", groupId = "inventory-group")
public void releaseInventory(Order order) {
    System.out.println("Releasing inventory for order: " + order.getId());
    Inventory inventory = repo.findById(order.getProductId()).get();

    inventory.setQuantity(inventory.getQuantity() + order.getQuantity()); //8 + 2
    repo.save(inventory); //10

    kafkaTemplate.send("inventory-released", order);
}

}

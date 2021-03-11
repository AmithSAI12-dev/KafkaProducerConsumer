package com.project.kafka.service;

import com.project.kafka.avro.Category;
import com.project.kafka.avro.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ConsumerService {

    @KafkaListener(topics = "category", groupId = "category_id")
    public void consumeMessage(Category category) {
        log.info("Consuming Message: ===========> {}", category);
    }

    @KafkaListener(topics = "product",  groupId = "product_id")
    public void consumeProductMessage(Product product) {
        log.info("Consuming message: =============?> {}", product);
    }


}


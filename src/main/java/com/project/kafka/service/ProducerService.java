package com.project.kafka.service;

import com.project.kafka.avro.Category;
import com.project.kafka.avro.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ProducerService {

    private final KafkaTemplate<String, Product> productKafkaTemplate;
    private final KafkaTemplate<String, Category> categoryKafkaTemplate;
    private static final String PRODUCTTOPIC = "product";
    private static final String CATEGORYTOPIC = "category";

    @Autowired
    public ProducerService(KafkaTemplate<String, Product> productKafkaTemplate, KafkaTemplate<String, Category> categoryKafkaTemplate) {
        this.productKafkaTemplate = productKafkaTemplate;
        this.categoryKafkaTemplate = categoryKafkaTemplate;
    }

    public void sendProduct(Product product) {
        log.info("Producing message =============> {}", product.toString());
        this.productKafkaTemplate.send(PRODUCTTOPIC, product);
    }

    public void sendCategory(Category category) {
        log.info("Producing message =============> {}", category.toString());
        this.categoryKafkaTemplate.send(CATEGORYTOPIC, category);
    }
}

package com.project.kafka.service;

import com.project.kafka.avro.Category;
import com.project.kafka.avro.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFutureCallback;

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
        this.productKafkaTemplate.send(PRODUCTTOPIC, product).addCallback(new ListenableFutureCallback<SendResult<String, Product>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.info("Exception Occurred: {}", ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Product> result) {
                log.info("Successfully Produced message {}", result.getProducerRecord().toString());
            }
        });
    }

    public void sendCategory(Category category) {
        log.info("Producing message =============> {}", category.toString());
        this.categoryKafkaTemplate.send(CATEGORYTOPIC, category).addCallback(new ListenableFutureCallback<SendResult<String, Category>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.info("Exception Occurred: {}", ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Category> result) {
                log.info("Successfully Produced message {}", result.getProducerRecord().toString());
            }
        });
    }
}

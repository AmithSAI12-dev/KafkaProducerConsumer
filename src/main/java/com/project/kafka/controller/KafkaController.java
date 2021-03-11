package com.project.kafka.controller;

import com.project.kafka.avro.Category;
import com.project.kafka.avro.Product;
import com.project.kafka.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    private final ProducerService producerService;

    @Autowired
    public KafkaController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping(value = "/product/publish")
    public void sendKafkaTopic(@RequestBody Product product) {
        producerService.sendProduct(product);
    }

    @PostMapping(value = "/category/publish")
    public void sendCategoryTopic(@RequestBody Category category) {
        producerService.sendCategory(category);
    }

}

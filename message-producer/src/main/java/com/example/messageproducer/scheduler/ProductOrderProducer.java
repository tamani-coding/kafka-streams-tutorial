package com.example.messageproducer.scheduler;

import com.example.messageproducer.products.Products;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ProductOrderProducer {

    @Autowired
    private KafkaTemplate<String, Integer> kafkaTemplate;

    @Value(value = "${kafka.topic.order}")
    private String topic;

    @Scheduled(fixedRate=8000)
    public void task() {
        String randomProduct = Products.randomProduct();
        System.out.println("sending product order event ... " + randomProduct);
        kafkaTemplate.send(topic, randomProduct, 1);
    }
}

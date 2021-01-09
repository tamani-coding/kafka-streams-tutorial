package com.example.messageproducer.scheduler;

import com.example.messageproducer.products.Products;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

@Service
public class OrderProducer {

    @Autowired
    private KafkaTemplate<String, Integer> kafkaTemplate;

    @Value(value = "${kafka.topic.order}")
    private String topic;

    @Scheduled(fixedRate=500)
    public void checkRecords() {
        System.out.println("sending order ...");
        kafkaTemplate.send(topic, Products.randomProduct(), ThreadLocalRandom.current().nextInt(1, 11));
    }
}

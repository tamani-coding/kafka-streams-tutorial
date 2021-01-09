package com.example.messageproducer.scheduler;

import com.example.messageproducer.products.Products;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

@Service
public class ReturnTopic {

    @Autowired
    private KafkaTemplate<String, Integer> kafkaTemplate;

    @Value(value = "${kafka.topic.return}")
    private String topic;

    @Scheduled(fixedRate=1000)
    public void checkRecords() {
        System.out.println("sending return ...");
        kafkaTemplate.send(topic, Products.randomProduct(), ThreadLocalRandom.current().nextInt(1, 3));
    }
}

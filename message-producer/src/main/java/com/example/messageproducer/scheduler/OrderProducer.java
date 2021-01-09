package com.example.messageproducer.scheduler;

import com.example.messageproducer.model.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    @Value(value = "${kafka.topic.order}")
    private String topic;

    @Scheduled(fixedRate=500)
    public void checkRecords() {
        System.out.println("sending order ...");
        kafkaTemplate.send(topic, Order.builder().product("book").amount(2).build());
    }
}

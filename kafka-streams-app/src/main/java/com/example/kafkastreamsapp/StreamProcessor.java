package com.example.kafkastreamsapp;

import com.example.messageproducer.model.Order;
import com.example.messageproducer.model.Return;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.handler.annotation.SendTo;

@EnableBinding(OrderAndReturnProcessor.class)
public class StreamProcessor {

    @StreamListener
    @SendTo(Processor.OUTPUT)
    public KStream<String, Long> process(@Input("input1")KStream<?, Order>  input1,
                                         @Input("input2")KStream<?, Return>  input2) {
        input1.foreach( (a,b) -> {
            System.out.println("order " + b.getProduct() + " " + b.getAmount());
        });

        input2.foreach( (a,b) -> {
            System.out.println("return " + b.getProduct() + " " + b.getAmount());
        });

        KTable<String, Long> count = input2.groupBy((key, aReturn) -> (aReturn.getProduct()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
        return count.toStream();

//        KTable<String, Long> wordCounts = input1
//                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
//                .groupBy((key, word) -> word)
//                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
//        return wordCounts.toStream();
    }

}

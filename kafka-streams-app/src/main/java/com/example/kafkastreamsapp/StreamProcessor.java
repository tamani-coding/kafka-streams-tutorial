package com.example.kafkastreamsapp;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.Arrays;

@EnableBinding(OrderAndReturnProcessor.class)
public class StreamProcessor {

    @StreamListener
    @SendTo(Processor.OUTPUT)
    public KStream<String, Long> process(@Input("input1")KStream<?, String>  input1, @Input("input2")KStream<?, String>  input2) {
        input1.foreach( (a,b) -> {
            System.out.println(b);
        });

        input2.foreach( (a,b) -> {
            System.out.println(b);
        });

        KTable<String, Long> wordCounts = input1
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
        return wordCounts.toStream();
    }

}

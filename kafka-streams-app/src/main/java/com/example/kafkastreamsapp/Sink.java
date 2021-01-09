package com.example.kafkastreamsapp;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;

public class Sink {

    @StreamListener(CustomSink.INPUT)
    public void process(KStream<String, String> input) {
        input.foreach( (a,b) -> {
            System.out.println("sink: " + a + " : " + b);
        });
    }

    interface CustomSink {
        String INPUT = "input-sink";

        @Input(INPUT)
        KStream<?, ?> input();
    }

}

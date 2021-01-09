package com.example.kafkastreamsapp;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;

public interface OrderAndReturnProcessor {

    @Input("input1")
    KStream<?, ?> input1();

    @Input("input2")
    KStream<?, ?> input2();

    @Output("output")
    KStream<?, ?> output();

}

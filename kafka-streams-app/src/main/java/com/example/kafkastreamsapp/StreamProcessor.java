package com.example.kafkastreamsapp;

import com.example.model.ViewOrderAggregate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.messaging.handler.annotation.SendTo;

@EnableBinding(OrderAndReturnProcessor.class)
public class StreamProcessor {

    @StreamListener
    @SendTo(Processor.OUTPUT)
    public KStream<String, ViewOrderAggregate> process(@Input("input1")KStream<String, String>  input1,
                                                       @Input("input2")KStream<String, String>  input2) {
//        input1.foreach( (a,b) -> {
//            System.out.println("view " + a + " " + b);
//        });
//
//        input2.foreach( (a,b) -> {
//            System.out.println("order " + a + " " + b);
//        });

        KTable<String, Integer> countViews = input1.groupBy((k, v) -> k).aggregate(
                () -> 0,
                (key, newValue, aggValue) -> Integer.valueOf(newValue) + aggValue,
                Materialized.with(Serdes.String(), Serdes.Integer()));

//        countViews.toStream().foreach( (a,b) -> {
//            System.out.println("countViews " + a + " " + b);
//        });

        KTable<String, Integer> countOrders = input2.groupBy((k, v) -> k).aggregate(
                () -> 0,
                (key, newValue, aggValue) -> Integer.valueOf(newValue) + aggValue,
                Materialized.with(Serdes.String(), Serdes.Integer()));

//        countOrders.toStream().foreach( (a,b) -> {
//            System.out.println("countOrders " + a + " " + b);
//        });

        KTable<String, ViewOrderAggregate> joined = countViews.leftJoin(countOrders, (leftValue, rightValue) -> ViewOrderAggregate.builder()
                        .amountViews(leftValue)
                        .amountOrders(rightValue != null ? rightValue : 0).build(), /* ValueJoiner */
                Materialized.with(Serdes.String(), new JsonSerde<>(ViewOrderAggregate.class)));

//        joined.toStream().foreach( (a, b)  -> {
//            System.out.println("joined: " + a + " " + b.getAmountViews() + " " + b.getAmountOrders());
//        });

        return joined.toStream();
    }

}

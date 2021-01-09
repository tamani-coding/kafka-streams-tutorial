package com.example.kafkastreamsapp;

import com.example.model.ViewOrderAggregate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.handler.annotation.SendTo;

import java.time.Duration;

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

        KStream<String, ViewOrderAggregate> joined = input1.leftJoin(input2,
                (leftValue, rightValue) -> ViewOrderAggregate
                                            .builder()
                                            .amountViews(Integer.valueOf(leftValue))
                                            .amountOrders(rightValue != null ? Integer.valueOf(rightValue) : 0).build(), /* ValueJoiner */
                JoinWindows.of(Duration.ofMinutes(5)),
                StreamJoined.with(
                        Serdes.String(), /* key */
                        Serdes.String(),   /* left value */
                        Serdes.String())  /* right value */
        );

//        joined.foreach( (a, b)  -> {
//            System.out.println("joined: " + a + " " + b.getAmountViews() + " " + b.getAmountOrders());
//        });

        KTable<String, ViewOrderAggregate> ktable = joined.groupBy( (key, value) -> key)
              .aggregate(
                      () -> ViewOrderAggregate.builder().build(),
                      (key, newValue, aggValue) -> {
                        return ViewOrderAggregate.builder()
                                .amountViews(aggValue.getAmountViews() + newValue.getAmountViews())
                                .amountOrders(aggValue.getAmountOrders() + newValue.getAmountOrders()).build();
                      },
                      Materialized.as("product-aggregate"));

//        ktable.toStream().foreach( (key, value) -> {
//            System.out.println("aggregated " + key + " " + value);
//        });

        return ktable.toStream();
    }

}

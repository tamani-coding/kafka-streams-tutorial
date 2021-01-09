package com.example.kafkastreamsapp;

import com.example.model.OrderReturnAggregate;
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
    public KStream<String, OrderReturnAggregate> process(@Input("input1")KStream<String, String>  input1,
                                                         @Input("input2")KStream<String, String>  input2) {
//        input1.foreach( (a,b) -> {
//            System.out.println("order " + a + " " + b);
//        });
//
//        input2.foreach( (a,b) -> {
//            System.out.println("return " + a + " " + b);
//        });

        KStream<String, OrderReturnAggregate> joined = input1.join(input2,
                (leftValue, rightValue) -> OrderReturnAggregate
                                            .builder()
                                            .amountOrders(Integer.valueOf(leftValue))
                                            .amountReturns(Integer.valueOf(rightValue)).build(), /* ValueJoiner */
                JoinWindows.of(Duration.ofMinutes(60)),
                StreamJoined.with(
                        Serdes.String(), /* key */
                        Serdes.String(),   /* left value */
                        Serdes.String())  /* right value */
        );

//        joined.foreach( (a, b)  -> {
//            System.out.println("joined: " + a + " " + b.getAmountOrders() + " " + b.getAmountReturns());
//        });

        KTable<String, OrderReturnAggregate> ktable = joined.groupBy( (key, value) -> key)
              .aggregate(
                      () -> OrderReturnAggregate.builder().build(),
                      (key, newValue, aggValue) -> {
                        return OrderReturnAggregate.builder()
                                .amountOrders(aggValue.getAmountOrders() + newValue.getAmountOrders())
                                .amountReturns(aggValue.getAmountReturns() + newValue.getAmountReturns()).build();
                      },
                      Materialized.as("product-aggregate"));

//        ktable.toStream().foreach( (key, value) -> {
//            System.out.println("aggregated " + key + " " + value);
//        });

        return ktable.toStream();
    }

}

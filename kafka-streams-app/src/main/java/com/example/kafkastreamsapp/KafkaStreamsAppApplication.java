package com.example.kafkastreamsapp;

import com.example.model.ViewOrderAggregate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.function.BiFunction;

@SpringBootApplication
public class KafkaStreamsAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsAppApplication.class, args);
	}

	@Bean
	public BiFunction<KStream<String, String>, KStream<String, String>, KStream<String, ViewOrderAggregate>> viewOrderAggregate() {
		return (input1, input2) -> {
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
		};
	}
}

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
		return (views, orders) -> {
//			views.foreach( (a,b) -> {
//				System.out.println("view " + a + " " + b);
//			});
//
//			orders.foreach( (a,b) -> {
//				System.out.println("order " + a + " " + b);
//			});

			// aggregate views per product
			KTable<String, Integer> countViews = views.groupBy((k, v) -> k).aggregate(
					() -> 0,
					(key, newValue, aggValue) -> Integer.valueOf(newValue) + aggValue,
					Materialized.with(Serdes.String(), Serdes.Integer()));

//			countViews.toStream().foreach( (a,b) -> {
//				System.out.println("countViews " + a + " " + b);
//			});

			// aggregate orders per product
			KTable<String, Integer> countOrders = orders.groupBy((k, v) -> k).aggregate(
					() -> 0,
					(key, newValue, aggValue) -> Integer.valueOf(newValue) + aggValue,
					Materialized.with(Serdes.String(), Serdes.Integer()));

//			countOrders.toStream().foreach( (a,b) -> {
//				System.out.println("countOrders " + a + " " + b);
//			});

			// left join tables
			KTable<String, ViewOrderAggregate> joined = countViews.leftJoin(countOrders, (leftValue, rightValue) -> ViewOrderAggregate.builder()
							.amountViews(leftValue)
							.amountOrders(rightValue != null ? rightValue : 0).build(), /* ValueJoiner */
					Materialized.with(Serdes.String(), new JsonSerde<>(ViewOrderAggregate.class)));

//			joined.toStream().foreach( (a, b)  -> {
//				System.out.println("joined: " + a + " " + b.getAmountViews() + " " + b.getAmountOrders());
//			});

			// stream joined table
			return joined.toStream();
		};
	}
}

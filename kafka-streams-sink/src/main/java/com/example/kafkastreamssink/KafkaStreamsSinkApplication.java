package com.example.kafkastreamssink;

import com.example.model.ViewOrderAggregate;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;

@SpringBootApplication
public class KafkaStreamsSinkApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsSinkApplication.class, args);
	}

	@Bean
	public Consumer<KStream<String, ViewOrderAggregate>> consumeAggregate() {
		return (input) -> {
			input.foreach( (a,b) -> {
				System.out.println("consuming: " + a + " : " + b);
			});
		};
	}
}

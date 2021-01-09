package com.example.messageproducer.products;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Products {

    private static List<String> products = List.of("book", "laptop", "display", "chair", "headset", "desk");

    public static String randomProduct () {
        return products.get(ThreadLocalRandom.current().nextInt(0, products.size()));
    }
}

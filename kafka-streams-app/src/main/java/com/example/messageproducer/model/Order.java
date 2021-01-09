package com.example.messageproducer.model;

import lombok.Builder;
import lombok.Data;

@Data
public class Order {

    private String product;
    private int amount;
}

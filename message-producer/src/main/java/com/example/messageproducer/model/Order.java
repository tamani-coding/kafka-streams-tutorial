package com.example.messageproducer.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Order {

    private String product;
    private int amount;

}

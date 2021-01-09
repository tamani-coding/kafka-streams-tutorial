package com.example.kafkastreamsapp.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OrderReturnAggregate {

    private String product;
    private int amountOrders;
    private int amountReturns;

}

package com.example.kafkastreamsapp;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

@Data
@Builder

public class OrderReturnAggregate {

    private int amountOrders;
    private int amountReturns;

    @Tolerate
    public OrderReturnAggregate() {}
}

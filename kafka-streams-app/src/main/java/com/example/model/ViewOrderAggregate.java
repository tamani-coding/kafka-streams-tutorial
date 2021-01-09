package com.example.model;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;

@Data
@Builder

public class ViewOrderAggregate {

    private int amountViews;
    private int amountOrders;

    @Tolerate
    public ViewOrderAggregate() {}
}

package com.example.messageproducer.model;

import lombok.Builder;
import lombok.Data;

public class Order {

    private String product;
    private int amount;

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }
}

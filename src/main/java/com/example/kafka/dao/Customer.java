package com.example.kafka.dao;

import lombok.Data;

@Data
public class Customer {

    public Customer(int customerId,String customerName){
        this.customerId = customerId;
        this.customerName = customerName;
    }
    private int customerId;
    private String customerName ;
}

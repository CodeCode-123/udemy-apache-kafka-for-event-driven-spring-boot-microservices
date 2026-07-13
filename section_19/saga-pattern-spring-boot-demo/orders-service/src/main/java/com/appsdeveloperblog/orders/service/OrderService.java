package com.appsdeveloperblog.orders.service;

import com.appsdeveloperblog.core.dto.Order;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public interface OrderService {
    Order placeOrder(Order order) throws ExecutionException, InterruptedException;
    void approveOrder(UUID orderId);
}

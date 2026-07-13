package com.appsdeveloperblog.orders.service.handler;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics="${orders.commands.topic.name}")
public class OrderCommandsHandler {
}

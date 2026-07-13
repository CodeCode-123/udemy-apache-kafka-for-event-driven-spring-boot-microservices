package com.appsdeveloperblog.orders.saga;

import com.appsdeveloperblog.core.dto.commands.ApprovedOrderCommand;
import com.appsdeveloperblog.core.dto.events.OrderApprovedEvent;
import com.appsdeveloperblog.core.dto.events.PaymentProcessedEvent;
import com.appsdeveloperblog.core.dto.events.ProductReservedEvent;
import com.appsdeveloperblog.core.types.OrderStatus;
import com.appsdeveloperblog.orders.service.OrderHistoryService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import com.appsdeveloperblog.core.dto.events.OrderCreatedEvent;
import com.appsdeveloperblog.core.dto.commands.ReserveProductCommand;
import org.springframework.transaction.annotation.Transactional;
import com.appsdeveloperblog.core.dto.commands.ProcessPaymentCommand;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

//act as a central orchestrator, making sure all steps are executed in the correct order
@Component
@KafkaListener(topics = {
        "${orders.events.topic.name}",
        "${products.events.topic.name}",
        "${payments.events.topic.name}"
})
public class OrderSaga {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String productsCommandsTopicName;
    private final OrderHistoryService orderHistoryService;
    private final Logger LOGGER  = LoggerFactory.getLogger(this.getClass());
    private final String paymentsCommandsTopicName;
    private final String ordersCommandsTopicName;

    String keyId = UUID.randomUUID().toString();

    public OrderSaga(KafkaTemplate<String, Object> kafkaTemplate,
                     @Value("${products.commands.topic.name}") String productsCommandsTopicName,
                     OrderHistoryService orderHistoryService,
                     @Value("${payments.commands.topic.name}") String paymentsCommandsTopicName,
                     @Value("${orders.commands.topic.name}") String ordersCommandsTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.productsCommandsTopicName = productsCommandsTopicName;
        this.orderHistoryService = orderHistoryService;
        this.paymentsCommandsTopicName = paymentsCommandsTopicName;
        this.ordersCommandsTopicName = ordersCommandsTopicName;
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderCreatedEvent event, @Header("messageId") String messageId,
                            @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) throws ExecutionException, InterruptedException {
        LOGGER.info("Received a new event: with orderId: {} and productId: {}", event.getOrderId(), event.getProductId());

        ReserveProductCommand command = new ReserveProductCommand(
                event.getProductId(),
                event.getProductQuantity(),
                event.getOrderId()
        );
        ProducerRecord<String, Object> record = new ProducerRecord<>(
                "products-commands",
                keyId,
                command);
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());
        LOGGER.info("Before publishing a ReserveProductCommand");
        SendResult<String, Object> result = kafkaTemplate.send(record).get();
        LOGGER.info("Partition: {}", result.getRecordMetadata().partition());
        LOGGER.info("Topic: {}", result.getRecordMetadata().topic());
        LOGGER.info("Offset: {}", result.getRecordMetadata().offset());
        orderHistoryService.add(event.getOrderId(), OrderStatus.CREATED);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservedEvent event) {
        ProcessPaymentCommand processPaymentCommand = new ProcessPaymentCommand(event.getOrderId(),
                event.getProductId(), event.getProductPrice(), event.getProductQuantity());
        kafkaTemplate.send(paymentsCommandsTopicName, processPaymentCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload PaymentProcessedEvent event) {
        ApprovedOrderCommand approvedOrderCommand = new ApprovedOrderCommand(event.getOrderId());
        kafkaTemplate.send(ordersCommandsTopicName, approvedOrderCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderApprovedEvent event) {
        orderHistoryService.add(event.getOrderId(), OrderStatus.APPROVED);
    }
}

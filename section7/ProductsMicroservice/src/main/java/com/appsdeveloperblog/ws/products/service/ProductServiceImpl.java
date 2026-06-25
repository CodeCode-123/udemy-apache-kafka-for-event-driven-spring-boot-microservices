package com.appsdeveloperblog.ws.products.service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;

@Service
public class ProductServiceImpl implements ProductService {
	private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
	public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public String createProduct(CreateProductRestModel productRestModel) throws Exception {
		String productId = UUID.randomUUID().toString();
		// TODO Persist Product Details into database table before publishing an Event
		ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(
				productId,
				productRestModel.getTitle(), 
				productRestModel.getPrice(),
				productRestModel.getQuantity());
		//kafkaTemplate send template: topic name, message key, and event object
		//send messages asynchronously
//		CompletableFuture<SendResult<String, ProductCreatedEvent>> future = 
//				kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent);
//		//handle the result using whenComplete method
//		future.whenComplete((result, exception) -> {
//			if (exception != null) {
//				LOGGER.error("****** Failed to send message: " + exception.getMessage());
//			} else {
//				LOGGER.info("****** Message sent successfully: " + result.getRecordMetadata());
//			}
//		});
		
		// This method will block the current thread until the future is complete
		// it will become synchronous
		//future.join();
		
		LOGGER.info("Before publishing a ProductCreatedEvent");
		
		//send message synchronously
		SendResult<String, ProductCreatedEvent> result = 
				kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent).get();
		LOGGER.info("Partition: " + result.getRecordMetadata().partition());
		LOGGER.info("Topic: " + result.getRecordMetadata().topic());
		LOGGER.info("Offset: " + result.getRecordMetadata().offset());
		
		LOGGER.info("******* Returning product id");
		
		return productId;
	}

}

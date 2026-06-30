package com.appsdeveloperblog.ws.products.service;

import java.util.UUID;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import com.appsdevloperblog.ws.core.ProductCreatedEvent;

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
		
		LOGGER.info("Before publishing a ProductCreatedEvent");
		
		//ProducerRecorder accept message header as a parameter
		ProducerRecord<String, ProductCreatedEvent> record = new ProducerRecord<>(
				"product-created-events-topic",
				productId,
				productCreatedEvent);
		//use message key and message id separately, use product Id as message key
		//use message Id as another unique identifier included in the message header
		//and this unique id will be stored in a database to prevent processing and duplicate message
		record.headers().add("messageId", UUID.randomUUID().toString().getBytes());
		//hard coded id will generate duplicated id
		//record.headers().add("messageId", "123".getBytes());

		//send message synchronously
		SendResult<String, ProductCreatedEvent> result = 
				kafkaTemplate.send(record).get();
		LOGGER.info("Partition: " + result.getRecordMetadata().partition());
		LOGGER.info("Topic: " + result.getRecordMetadata().topic());
		LOGGER.info("Offset: " + result.getRecordMetadata().offset());
		
		LOGGER.info("******* Returning product id");
		
		return productId;
	}

}

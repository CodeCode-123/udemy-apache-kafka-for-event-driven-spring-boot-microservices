package com.appsdevloperblog.ws.emailnotification.handler;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.appsdevloperblog.ws.core.ProductCreatedEvent;
import com.appsdevloperblog.ws.emailnotification.error.NotRetryableException;
import com.appsdevloperblog.ws.emailnotification.error.RetryableException;
import com.appsdevloperblog.ws.emailnotification.io.ProcessedEventEntity;
import com.appsdevloperblog.ws.emailnotification.io.ProcessedEventRepository;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	private RestTemplate restTemplate;
	private ProcessedEventRepository processedEventRepository;

	public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
		this.restTemplate = restTemplate;
		this.processedEventRepository = processedEventRepository;
	}

	//@Payload annotation is used to bind method arguments to the payload of a message, and this means 
	//the data inside of message payload, it will be converted to product created event object, it will be injected
	//to this method as method argument. To read message header I can use header annotation.
	@Transactional
	@KafkaHandler 
	public void handle(@Payload ProductCreatedEvent productCreatedEvent, 
			@Header("messageId") String messageId, 
			//KafkaHeaders.RECEIVED_KEY is the message key
			@Header(KafkaHeaders.RECEIVED_KEY) String messageKey) { 
		LOGGER.info("Received a new event: " + productCreatedEvent.getTitle() + " with productId: "
				+ productCreatedEvent.getProductId());
		
		//Check if this message was already processed before
		ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);
		
		if (existingRecord != null) {
			LOGGER.info("Found a duplicate messsage id: {}", existingRecord.getMessageId());
			return;
		}
		
		String requestUrl = "http://localhost:8082/response/200";
		
		try {
			ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);
					
		   //restClient.exchange(requestUrl, HttpMethod.GET, null, String.class);
			if (response.getStatusCode().value() == HttpStatus.OK.value()) {
				LOGGER.info("Received response from a remote service: " + response.getBody());
			}
		} catch (ResourceAccessException ex) {
			// if remote microservice is not available, throw a retry exception
			LOGGER.error(ex.getMessage());
			throw new RetryableException(ex);
		} catch (HttpServerErrorException ex) {
			// if the remote microservice responds with 500 error, throw a not retry
			// exception
			LOGGER.error(ex.getMessage());
			throw new NotRetryableException(ex);
		} catch (Exception ex) {
			LOGGER.error(ex.getMessage());
			throw new NotRetryableException(ex);
		}
		
		//Save a unique message id in a database table, 
		//and it is not allowed to save duplicated message id in the table
		try {
			processedEventRepository.save(new ProcessedEventEntity(messageId, 
					productCreatedEvent.getProductId()));
		} catch(DataIntegrityViolationException ex) {
			throw new NotRetryableException(ex);
		}
		
	}

}

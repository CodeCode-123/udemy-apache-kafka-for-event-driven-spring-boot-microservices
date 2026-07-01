package com.appsdeveloperblog.estore.transfers.service;

import java.net.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaOperations.OperationsCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import com.appsdeveloperblog.estore.transfers.error.TransferServiceException;
import com.appsdeveloperblog.estore.transfers.model.TransferRestModel;
import com.appsdeveloperblog.payments.ws.core.events.DepositRequestedEvent;
import com.appsdeveloperblog.payments.ws.core.events.WithdrawalRequestedEvent;

@Service
public class TransferServiceImpl implements TransferService {
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private KafkaTemplate<String, Object> kafkaTemplate;
	private Environment environment;
	private RestTemplate restTemplate;

	public TransferServiceImpl(KafkaTemplate<String, Object> kafkaTemplate, Environment environment,
			RestTemplate restTemplate) {
		this.kafkaTemplate = kafkaTemplate;
		this.environment = environment;
		this.restTemplate = restTemplate;
	}

	//If there are more than one transaction manager, indicate which one will be used
	//@Transactional is used for rollback during failure, by default rollback runtime exception
	//to rollback checked exceptions, they should be defined in this method
//	@Transactional(value="kafkaTransactionManager", 
//			rollbackFor= { TransferServiceException.class, ConnectException.class },
//			noRollbackFor= { SpecificException.class })
	//@Transactional annotation is managed by Spring Framework
	@Transactional
	@Override
	public boolean transfer(TransferRestModel transferRestModel) {
		WithdrawalRequestedEvent withdrawalEvent = new WithdrawalRequestedEvent(transferRestModel.getSenderId(),
				transferRestModel.getRecepientId(), transferRestModel.getAmount());
		DepositRequestedEvent depositEvent = new DepositRequestedEvent(transferRestModel.getSenderId(),
				transferRestModel.getRecepientId(), transferRestModel.getAmount());

		try {
//			kafkaTemplate.send(environment.getProperty("withdraw-money-topic", "withdraw-money-topic"),
//					withdrawalEvent);
//			kafkaTemplate.executeInTransaction((OperationsCallback<String, Object, T>) operations -> {
//				operations.send(environment.getProperty("withdraw-money-topic", "withdraw-money-topic"), withdrawalEvent);
//				return null;
//			});
			//kafkaTemplate.executeInTransaction is managed by Kafka's internal Transaction manager
			//It is a local transaction, if it is successful, it will not rollback if there is an exception outside this local transaction 
			boolean returnValue = kafkaTemplate.executeInTransaction(t -> {
				t.send("withdraw-money-topic", withdrawalEvent);
				return true;
			});
			LOGGER.info("Sent event to withdrawal topic.");

			// Business logic that causes and error
			callRemoteServce();

//			kafkaTemplate.send(environment.getProperty("deposit-money-topic", "deposit-money-topic"), depositEvent);
//			kafkaTemplate.executeInTransaction((OperationsCallback<String, Object, T>) operations -> {
//				operations.send(environment.getProperty("deposit-money-topic", "deposit-money-topic"), depositEvent);
//				return null;
//			});
			returnValue = kafkaTemplate.executeInTransaction(t -> {
				t.send("deposit-money-topic", depositEvent);
				return true;
			});
			LOGGER.info("Sent event to deposit topic");

		} catch (Exception ex) {
			LOGGER.error(ex.getMessage(), ex);
			throw new TransferServiceException(ex);
		}

		return true;
	}

	private ResponseEntity<String> callRemoteServce() throws Exception {
		String requestUrl = "http://localhost:8082/response/200";
		ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);

		if (response.getStatusCode().value() == HttpStatus.SERVICE_UNAVAILABLE.value()) {
			throw new Exception("Destination Microservice not availble");
		}

		if (response.getStatusCode().value() == HttpStatus.OK.value()) {
			LOGGER.info("Received response from mock service: " + response.getBody());
		}
		return response;
	}

}

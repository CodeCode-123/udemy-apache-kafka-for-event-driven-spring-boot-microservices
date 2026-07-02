package com.appsdeveloperblog.estore.transfers.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import com.appsdeveloperblog.estore.transfers.error.TransferServiceException;
import com.appsdeveloperblog.estore.transfers.io.TransferEntity;
import com.appsdeveloperblog.estore.transfers.io.TransferRepository;
import com.appsdeveloperblog.estore.transfers.model.TransferRestModel;
import com.appsdeveloperblog.payments.ws.core.events.DepositRequestedEvent;
import com.appsdeveloperblog.payments.ws.core.events.WithdrawalRequestedEvent;

@Service
public class TransferServiceImpl implements TransferService {
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private KafkaTemplate<String, Object> kafkaTemplate;
	private Environment environment;
	private RestTemplate restTemplate;
	private TransferRepository transferRepository;

	public TransferServiceImpl(KafkaTemplate<String, Object> kafkaTemplate, Environment environment,
			RestTemplate restTemplate, TransferRepository transferRepository) {
		this.kafkaTemplate = kafkaTemplate;
		this.environment = environment;
		this.restTemplate = restTemplate;
		this.transferRepository = transferRepository;
	}

	//It is necessary to configure kafkaTransactionManager with @Bean in KafkaConfig
	//This transactionManager will work with both Jpa and Kafka transaction
	@Transactional("transactionManager")
	@Override
	public boolean transfer(TransferRestModel transferRestModel) {
		WithdrawalRequestedEvent withdrawalEvent = new WithdrawalRequestedEvent(transferRestModel.getSenderId(),
				transferRestModel.getRecepientId(), transferRestModel.getAmount());
		DepositRequestedEvent depositEvent = new DepositRequestedEvent(transferRestModel.getSenderId(),
				transferRestModel.getRecepientId(), transferRestModel.getAmount());
		
		TransferEntity transferEntity = new TransferEntity();
		BeanUtils.copyProperties(transferRestModel, transferEntity);
		transferEntity.setTransferId(Uuid.randomUuid().toString());
		
		//String transferId = Uuid.randomUuid().toString();
		
		try {
			// Save record to a database table
			transferRepository.save(transferEntity);

//			kafkaTemplate.send(environment.getProperty("withdraw-money-topic", "withdraw-money-topic"),
//					withdrawalEvent);
			ProducerRecord<String, Object> withdrawalRecord = new ProducerRecord<>(
					"withdraw-money-topic",
					withdrawalEvent);
			withdrawalRecord.headers().add("messageId", Uuid.randomUuid().toString().getBytes());
			SendResult<String, Object> withdrawalResult = kafkaTemplate.send(withdrawalRecord).get();
			LOGGER.info("Sent event to withdrawal topic.");
			LOGGER.info("Partition: " + withdrawalResult.getRecordMetadata().partition());
			LOGGER.info("Topic: " + withdrawalResult.getRecordMetadata().topic());
			LOGGER.info("Offset: " + withdrawalResult.getRecordMetadata().offset());
			
			callRemoteServce();
//			kafkaTemplate.send(environment.getProperty("deposit-money-topic", "deposit-money-topic"), depositEvent);
			ProducerRecord<String, Object> depositRecord = new ProducerRecord<>(
					"deposit-money-topic",
					depositEvent);
			depositRecord.headers().add("messageId", Uuid.randomUuid().toString().getBytes());
			SendResult<String, Object> depositResult = kafkaTemplate.send(depositRecord).get();
			LOGGER.info("Sent event to deposit topic");
			LOGGER.info("Partition: " + depositResult.getRecordMetadata().partition());
			LOGGER.info("Topic: " + depositResult.getRecordMetadata().topic());
			LOGGER.info("Offset: " + depositResult.getRecordMetadata().offset());
			
		} catch(Exception ex) {
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

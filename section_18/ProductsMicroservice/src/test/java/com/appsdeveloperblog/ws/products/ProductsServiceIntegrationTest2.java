package com.appsdeveloperblog.ws.products;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import com.appsdeveloperblog.ws.products.service.ProductService;
import com.appsdeveloperblog.ws.products.service.ProductServiceImpl;
import com.appsdevloperblog.ws.core.ProductCreatedEvent;

@ActiveProfiles("test") 
@EmbeddedKafka(topics = KafkaTemplateTests.TEMPLATE_TOPIC)
//@SpringBootTest(classes=ProductsMicroserviceApplication.class)
public class ProductsServiceIntegrationTest2 {
	
	 public static final String TEMPLATE_TOPIC = "product-created-events-topic-name";
	
	@Autowired
	 ProductService productService;
	
	public static EmbeddedKafkaBroker embeddedKafka;
	
	
	private static KafkaMessageListenerContainer<Integer, String> container;
	private static BlockingQueue<ConsumerRecord<String, ProductCreatedEvent>> records;
	
	@BeforeAll
	public static void setUp() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
		embeddedKafka.brokerProperties(getConsumerProperties());
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(embeddedKafka, "testT", false);
		DefaultKafkaConsumerFactory<Integer, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
		
		ContainerProperties containerProperties = new ContainerProperties("product-created-events-topic-name");
		container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
		records = new LinkedBlockingQueue<>();
		container.setupMessageListener((MessageListener<String, ProductCreatedEvent>) records::add);
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
		
	}

	@Test
	void testCreateProduct_whenGivenValidProductDetails_successfullySendsKafkaMessage() throws Exception {
		
		// Arrange 
		
		String title="iPhone 11";
		BigDecimal price = new BigDecimal(600);
		Integer quantity = 1;
		
		CreateProductRestModel createProductRestModel = new CreateProductRestModel();
		createProductRestModel.setPrice(price);
		createProductRestModel.setQuantity(quantity);
		createProductRestModel.setTitle(title);
		
		// Act
		productService.createProduct(createProductRestModel);
		
		
		// Assert
		ConsumerRecord<String, ProductCreatedEvent> message = records.poll(3000, TimeUnit.MILLISECONDS);
		assertNotNull(message);
		assertNotNull(message.key());
		ProductCreatedEvent productCreatedEvent = message.value();
		assertEquals(createProductRestModel.getQuantity(), productCreatedEvent.getQuantity());
		assertEquals(createProductRestModel.getTitle(), productCreatedEvent.getTitle());
		assertEquals(createProductRestModel.getPrice(), productCreatedEvent.getPrice());
	}
	
	
	private static Map<String, String> getConsumerProperties() {
		return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class.getName(),
				ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JacksonJsonDeserializer.class.getName(),
				ConsumerConfig.GROUP_ID_CONFIG, "product-created-events",
				JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.appsdeveloperblog.ws.core",
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
				);
	}
	
	@AfterAll
	public static void tearDown() {
		container.stop();
	}
	
	
}
package com.appsdeveloperblog.ws.products;

import org.springframework.boot.SpringApplication;


import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import com.appsdeveloperblog.ws.products.service.ProductServiceImpl;
import com.appsdeveloperblog.ws.core.ProductCreatedEvent;

@SpringBootApplication
public class ProductsMicroserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProductsMicroserviceApplication.class, args);
	}
	
	@Bean(name="productServiceImpl")
	ProductServiceImpl getProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate){
		return new ProductServiceImpl(kafkaTemplate);
	}

}

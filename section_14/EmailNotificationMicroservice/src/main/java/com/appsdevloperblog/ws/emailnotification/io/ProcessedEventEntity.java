package com.appsdevloperblog.ws.emailnotification.io;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name="processed-events")
public class ProcessedEventEntity implements Serializable {

	private static final long serialVersionUID = -270322488053563286L;
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name="id")
	private long id;
	
	@Column(nullable=false, unique=true)
	private String messageId;
	
	@Column(nullable=false)
	private String productId;
	
	public ProcessedEventEntity() {
	}

	public ProcessedEventEntity(String messageId, String productId) {
		this.messageId = messageId;
		this.productId = productId;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}
	
	

}

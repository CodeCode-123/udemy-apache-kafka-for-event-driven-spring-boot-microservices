package com.appsdevloperblog.ws.emailnotification.error;

public class RetryableException extends RuntimeException {
    //throw this error with a custom error message
	public RetryableException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	//accept object of the original exception that caused an error
	public RetryableException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}
	
}

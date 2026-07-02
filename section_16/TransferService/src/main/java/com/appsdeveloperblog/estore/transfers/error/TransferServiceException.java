package com.appsdeveloperblog.estore.transfers.error;

public class TransferServiceException extends RuntimeException {
    private static final long serialVersionUID = 1L;

	public TransferServiceException(Throwable cause) {
        super(cause);
    }

    public TransferServiceException(String message) {
        super(message);
    }
}

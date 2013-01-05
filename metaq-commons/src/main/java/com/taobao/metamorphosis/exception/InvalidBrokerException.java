package com.taobao.metamorphosis.exception;

public class InvalidBrokerException extends RuntimeException {
    static final long serialVersionUID = -1L;


    public InvalidBrokerException() {
        super();

    }


    public InvalidBrokerException(final String message, final Throwable cause) {
        super(message, cause);

    }


    public InvalidBrokerException(final String message) {
        super(message);

    }


    public InvalidBrokerException(final Throwable cause) {
        super(cause);

    }

}

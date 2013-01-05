package com.taobao.metamorphosis.exception;

public class InvalidMessageException extends MetaClientException {
    static final long serialVersionUID = -1L;


    public InvalidMessageException() {
        super();

    }


    public InvalidMessageException(String message, Throwable cause) {
        super(message, cause);

    }


    public InvalidMessageException(String message) {
        super(message);

    }


    public InvalidMessageException(Throwable cause) {
        super(cause);

    }

}

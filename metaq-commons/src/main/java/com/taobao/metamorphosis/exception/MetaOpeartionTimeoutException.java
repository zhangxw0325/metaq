package com.taobao.metamorphosis.exception;

public class MetaOpeartionTimeoutException extends MetaClientException {
    static final long serialVersionUID = -1L;


    public MetaOpeartionTimeoutException() {
        super();

    }


    public MetaOpeartionTimeoutException(String message, Throwable cause) {
        super(message, cause);

    }


    public MetaOpeartionTimeoutException(String message) {
        super(message);

    }


    public MetaOpeartionTimeoutException(Throwable cause) {
        super(cause);

    }

}

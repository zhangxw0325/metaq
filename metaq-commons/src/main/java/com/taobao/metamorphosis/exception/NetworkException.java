package com.taobao.metamorphosis.exception;

public class NetworkException extends MetaClientException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;


    public NetworkException() {
        super();

    }


    public NetworkException(String message, Throwable cause) {
        super(message, cause);

    }


    public NetworkException(String message) {
        super(message);

    }


    public NetworkException(Throwable cause) {
        super(cause);

    }

}

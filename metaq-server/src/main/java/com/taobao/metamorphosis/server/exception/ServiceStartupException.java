package com.taobao.metamorphosis.server.exception;

public class ServiceStartupException extends RuntimeException {

    public ServiceStartupException() {
        super();

    }


    public ServiceStartupException(final String message, final Throwable cause) {
        super(message, cause);

    }


    public ServiceStartupException(final String message) {
        super(message);

    }


    public ServiceStartupException(final Throwable cause) {
        super(cause);

    }

}

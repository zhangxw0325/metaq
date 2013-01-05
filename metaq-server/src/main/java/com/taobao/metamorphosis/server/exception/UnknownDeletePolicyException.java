package com.taobao.metamorphosis.server.exception;

public class UnknownDeletePolicyException extends RuntimeException {

    private static final long serialVersionUID = 4426831758552509034L;


    public UnknownDeletePolicyException() {
        super();

    }


    public UnknownDeletePolicyException(String message, Throwable cause) {
        super(message, cause);

    }


    public UnknownDeletePolicyException(String message) {
        super(message);

    }


    public UnknownDeletePolicyException(Throwable cause) {
        super(cause);

    }

}

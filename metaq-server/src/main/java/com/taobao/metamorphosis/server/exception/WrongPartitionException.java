package com.taobao.metamorphosis.server.exception;

public class WrongPartitionException extends IllegalArgumentException {

    public WrongPartitionException() {
        super();

    }


    public WrongPartitionException(String message, Throwable cause) {
        super(message, cause);

    }


    public WrongPartitionException(String s) {
        super(s);

    }


    public WrongPartitionException(Throwable cause) {
        super(cause);

    }

}

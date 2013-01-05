package com.taobao.metamorphosis.exception;

public class InvalidSystemClock extends RuntimeException {

    public InvalidSystemClock() {
        super();

    }


    public InvalidSystemClock(String message, Throwable cause) {
        super(message, cause);

    }


    public InvalidSystemClock(String message) {
        super(message);

    }


    public InvalidSystemClock(Throwable cause) {
        super(cause);

    }

}

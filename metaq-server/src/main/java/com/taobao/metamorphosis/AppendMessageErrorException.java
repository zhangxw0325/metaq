package com.taobao.metamorphosis;

public class AppendMessageErrorException extends RuntimeException {

    public AppendMessageErrorException() {
        super();

    }


    public AppendMessageErrorException(String message, Throwable cause) {
        super(message, cause);

    }


    public AppendMessageErrorException(String message) {
        super(message);

    }


    public AppendMessageErrorException(Throwable cause) {
        super(cause);

    }

}

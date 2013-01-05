package com.taobao.metamorphosis.server.exception;

public class MetamorphosisServerStartupException extends RuntimeException {

    static final long serialVersionUID = -1L;


    public MetamorphosisServerStartupException() {
        super();

    }


    public MetamorphosisServerStartupException(String message, Throwable cause) {
        super(message, cause);

    }


    public MetamorphosisServerStartupException(String message) {
        super(message);

    }


    public MetamorphosisServerStartupException(Throwable cause) {
        super(cause);

    }

}

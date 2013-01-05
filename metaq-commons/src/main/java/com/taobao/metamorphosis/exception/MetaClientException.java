package com.taobao.metamorphosis.exception;



/**
 * 客户端异常基类
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public class MetaClientException extends Exception {
    static final long serialVersionUID = -1L;


    public MetaClientException() {
        super();

    }


    public MetaClientException(String message, Throwable cause) {
        super(message, cause);

    }


    public MetaClientException(String message) {
        super(message);

    }


    public MetaClientException(Throwable cause) {
        super(cause);

    }

}

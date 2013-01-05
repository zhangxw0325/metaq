package com.taobao.metamorphosis.tools.utils;

/**
 * 代表jmx相关异常
 * 
 * @author 无花
 * @since 2011-8-23 下午5:19:56
 */

public class JMXClientException extends Exception {

    private static final long serialVersionUID = -7410016800727397507L;


    public JMXClientException(String message) {
        super(message);
    }


    public JMXClientException(Throwable cause) {
        super(cause);
    }


    public JMXClientException(String message, Throwable cause) {
        super(message, cause);
    }
}

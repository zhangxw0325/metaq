package com.taobao.metamorphosis.metaslave;
/**
 * 代表一个启动订阅master消息时的错误
 * @author 无花
 * @since 2011-6-28 下午03:35:30
 */

public class SubscribeMasterMessageException extends RuntimeException {

    private static final long serialVersionUID = 3449735809236405427L;

    public SubscribeMasterMessageException() {
        super();

    }


    public SubscribeMasterMessageException(String message, Throwable cause) {
        super(message, cause);

    }


    public SubscribeMasterMessageException(String message) {
        super(message);

    }


    public SubscribeMasterMessageException(Throwable cause) {
        super(cause);

    }
}

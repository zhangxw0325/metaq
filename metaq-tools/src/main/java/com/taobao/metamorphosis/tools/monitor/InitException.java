package com.taobao.metamorphosis.tools.monitor;

/**
 * 代表监控系统启动初始化间段出现的异常
 * @author 无花
 * @since 2011-5-24 下午05:10:42
 */

public class InitException extends Exception {

    private static final long serialVersionUID = 5811163916323040678L;

    public InitException() {
        super();

    }

    public InitException(String message, Throwable cause) {
        super(message, cause);

    }

    public InitException(String s) {
        super(s);

    }

    public InitException(Throwable cause) {
        super(cause);

    }

}

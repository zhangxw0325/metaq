package com.taobao.metamorphosis.tools.monitor.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 探测线程专用task,确保不会抛出异常,以免schedule停止后续探测
 * 
 * @author 无花
 * @since 2011-5-30 下午01:56:49
 */

public abstract class ProbTask implements Runnable {
    protected Log log = LogFactory.getLog(this.getClass());


    public void run() {
        try {
            this.doExecute();
        }
        catch (InterruptedException e) {
            this.log.warn("探测线程接收到中断信号.");
            Thread.currentThread().interrupt();
        }
        catch (Throwable e) {
            // 捕获掉所有异常,以免ScheduledExecutorService不能执行后续探测任务
            this.handleExceptionInner(e);
        }
    }


    private void handleExceptionInner(Throwable e) {
        try {
            this.handleException(e);
        }
        catch (Throwable e2) {
            // ignore
        }
    }


    abstract protected void doExecute() throws Exception;


    abstract protected void handleException(Throwable e);

}

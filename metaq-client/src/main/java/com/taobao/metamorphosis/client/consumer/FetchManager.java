package com.taobao.metamorphosis.client.consumer;

/**
 * Fetch请求管理器接口
 * 
 * @author boyan
 * @Date 2011-5-4
 * 
 */
public interface FetchManager {
    /**
     * 停止fetch
     * 
     * @throws InterruptedException
     */
    public void stopFetchRunner() throws InterruptedException;


    /**
     * 重设状态，重设状态后可重用并start
     */
    public void resetFetchState();


    /**
     * 启动管理器
     */
    public void startFetchRunner();


    /**
     * 添加fetch请求
     * 
     * @param request
     */
    public void addFetchRequest(FetchRequest request);


    /**
     * 是否关闭
     * 
     * @return
     */
    public boolean isShutdown();
}

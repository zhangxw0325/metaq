package com.taobao.metamorphosis.client.producer;



/**
 * 发送消息的回调
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-14
 * 
 */
public interface SendMessageCallback {

    /**
     * 当消息发送返回后回调，告知发送结果
     * 
     * @param result
     *            发送结果
     */
    public void onMessageSent(SendResult result);


    /**
     * 当发生异常的时候回调本方法
     * 
     * @param e
     */
    public void onException(Throwable e);

}

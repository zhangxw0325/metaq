package com.taobao.metamorphosis.client.consumer;

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;


/**
 * 异步消息监听器
 * 
 * @author boyan
 * @Date 2011-4-23
 * 
 */
public interface MessageListener {
    /**
     * 接收到消息列表，只有messages不为空并且不为null的情况下会触发此方法
     * 
     * @param messages
     *            TODO 拼写错误，应该是单数，暂时将错就错吧
     */
    public void recieveMessages(Message message);


    /**
     * 处理消息的线程池
     * 
     * @return
     */
    public Executor getExecutor();
}

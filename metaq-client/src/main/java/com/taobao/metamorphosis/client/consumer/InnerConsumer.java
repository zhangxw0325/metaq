package com.taobao.metamorphosis.client.consumer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 不对外提供的consumer接口，用于提供给Fetch使用
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-9-13
 * 
 */
public interface InnerConsumer {

    /**
     * 抓取消息
     * 
     * @param fetchRequest
     * @param timeout
     * @param timeUnit
     * @return
     * @throws MetaClientException
     * @throws InterruptedException
     */
    MessageIterator fetch(final FetchRequest fetchRequest, long timeout, TimeUnit timeUnit)
            throws MetaClientException, InterruptedException;


    /**
     * 兼容1.X与2.X版本
     */
    FetchResult fetchAll(final FetchRequest fetchRequest, long timeout, TimeUnit timeUnit)
            throws MetaClientException, InterruptedException;
    
    /**
     * 同比拉消息
     */
    DequeueResult fetchSync(final FetchRequest fetchRequest, long timeout, TimeUnit timeUnit)
            throws MetaClientException, InterruptedException;


    /**
     * 返回topic对应的消息监听器
     * 
     * @param topic
     * @return
     */
    MessageListener getMessageListener(final String topic);


    /**
     * 处理无法被客户端消费的消息
     * 
     * @param message
     * @throws IOException
     */
    void appendCouldNotProcessMessage(final Message message) throws IOException;


    /**
     * 查询offset
     * 
     * @param fetchRequest
     * @return
     * @throws MetaClientException
     */
    long offset(final FetchRequest fetchRequest) throws MetaClientException;

}

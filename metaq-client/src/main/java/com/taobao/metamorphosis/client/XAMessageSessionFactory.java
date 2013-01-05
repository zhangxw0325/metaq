package com.taobao.metamorphosis.client;

import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.client.producer.XAMessageProducer;


/**
 * 用于创建XA消息会话的工厂
 * 
 * @author boyan
 * 
 */
public interface XAMessageSessionFactory extends MessageSessionFactory {

    /**
     * 创建XA消息生产者
     * 
     * @param partitionSelector
     *            分区选择器
     * @return
     */
    public XAMessageProducer createXAProducer(PartitionSelector partitionSelector);


    /**
     * 创建XA消息生产者，默认使用轮询分区选择器
     * 
     * @return
     */
    public XAMessageProducer createXAProducer();

}

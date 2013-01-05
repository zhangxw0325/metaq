package com.taobao.metamorphosis.client.extension;

import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;


/**
 * 广播消息会话工厂,使用这个创建的Consumer在同一分组内的每台机器都能收到同一条消息,
 * 推荐一个应用只使用一个MessageSessionFactory
 * 
 * @author 无花
 * @since 2011-6-13 下午02:49:27
 */

public interface BroadcastMessageSessionFactory extends MessageSessionFactory {

    /**
     * 创建广播方式接收的消息消费者，offset将存储在本地
     * 
     * @param consumerConfig
     *            消费者配置
     * 
     * @return
     * */
    public MessageConsumer createBroadcastConsumer(ConsumerConfig consumerConfig);
}

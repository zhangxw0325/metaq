package com.taobao.metamorphosis.client.consumer;

import java.io.IOException;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.Shutdownable;


/**
 * 消费端的Recover管理器
 * 
 * @author 无花
 * @since 2011-10-31 下午3:40:04
 */

public interface RecoverManager extends Shutdownable {
    /**
     * 是否已经启动
     * 
     * @return
     */
    public boolean isStarted();


    /**
     * 启动recover
     * 
     * @param metaClientConfig
     */
    public void start(MetaClientConfig metaClientConfig);


    /**
     * 存入一个消息
     * 
     * @param group
     * @param message
     * @throws IOException
     */
    public void append(String group, Message message) throws IOException;
}

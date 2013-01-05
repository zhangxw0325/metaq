package com.taobao.metamorphosis.client.extension;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.RecoverManager;
import com.taobao.metamorphosis.client.consumer.storage.LocalOffsetStorage;
import com.taobao.metamorphosis.exception.InvalidConsumerConfigException;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.network.RemotingUtils;


/**
 * 广播消息会话工厂,使用这个创建的Consumer在同一分组内的每台机器都能收到同一条消息,
 * 推荐一个应用只使用一个MessageSessionFactory
 * 
 * @author 无花
 * @since 2011-6-13 下午02:44:24
 */

public class MetaBroadcastMessageSessionFactory extends MetaMessageSessionFactory implements
        BroadcastMessageSessionFactory {

    public MetaBroadcastMessageSessionFactory(final MetaClientConfig metaClientConfig) throws MetaClientException {
        super(metaClientConfig);
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.client.extension.BroadcastMessageSessionFactory
     * #createBroadcastConsumer
     * (com.taobao.metamorphosis.client.consumer.ConsumerConfig)
     */
    @Override
    public MessageConsumer createBroadcastConsumer(final ConsumerConfig consumerConfig) {
        return this.createBroadcastConsumer(consumerConfig, null);
    }


    protected MessageConsumer createBroadcastConsumer(final ConsumerConfig consumerConfig,
            final RecoverManager recoverManager) {
        // 先检查一次原始的group是否为空
        if (StringUtils.isBlank(consumerConfig.getGroup())) {
            throw new InvalidConsumerConfigException("Blank group");
        }

        return this.createConsumer(updateGroupForBroadcast(consumerConfig), this.newLocalOffsetStorage(),
            recoverManager);
    }


    private LocalOffsetStorage newLocalOffsetStorage() {
        try {
            return new LocalOffsetStorage();
        }
        catch (final IOException e) {
            throw new InvalidConsumerConfigException("创建Consumer失败,Create LocalOffsetStorage failed", e);
        }
    }


    static ConsumerConfig updateGroupForBroadcast(final ConsumerConfig consumerConfig) {
        try {
            consumerConfig.setGroup(consumerConfig.getGroup() + "-"
                    + RemotingUtils.getLocalAddress().replaceAll("[\\.\\:]", "-"));
            return consumerConfig;
        }
        catch (final Exception e) {
            throw new InvalidConsumerConfigException("获取本地ip失败", e);
        }
    }

}

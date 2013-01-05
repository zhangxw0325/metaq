package com.taobao.metamorphosis.metaslave;

import org.I0Itec.zkclient.ZkClient;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.client.consumer.ConsumerZooKeeper;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-6-27 ÏÂÎç06:44:48
 */

public class SlaveMetaMessageSessionFactory extends MetaMessageSessionFactory {

    private static int brokerId = -1;


    private SlaveMetaMessageSessionFactory(final MetaClientConfig metaClientConfig) throws MetaClientException {
        super(metaClientConfig);
    }


    public synchronized static SlaveMetaMessageSessionFactory create(final MetaClientConfig metaClientConfig,
            final int brokerId) throws MetaClientException {
        SlaveMetaMessageSessionFactory.brokerId = brokerId;

        return new SlaveMetaMessageSessionFactory(metaClientConfig);
    }


    @Override
    protected ConsumerZooKeeper initConsumerZooKeeper(final RemotingClientWrapper remotingClient,
            final ZkClient zkClient, final ZKConfig zkConfig) {
        if (SlaveMetaMessageSessionFactory.brokerId < 0) {
            throw new IllegalStateException("please set brokerId first");
        }
        return new SlaveConsumerZooKeeper(this.metaZookeeper, remotingClient, zkClient, zkConfig,
            SlaveMetaMessageSessionFactory.brokerId);
    }


    // for test
    static int getBrokerId() {
        return SlaveMetaMessageSessionFactory.brokerId;
    }

}

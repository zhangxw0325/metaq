package com.taobao.metamorphosis.client;

import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.client.producer.RoundRobinPartitionSelector;
import com.taobao.metamorphosis.client.producer.SimpleXAMessageProducer;
import com.taobao.metamorphosis.client.producer.XAMessageProducer;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 支持事务的XA消息工厂
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-17
 * 
 */
public class XAMetaMessageSessionFactory extends MetaMessageSessionFactory implements XAMessageSessionFactory {

    public XAMetaMessageSessionFactory(final MetaClientConfig metaClientConfig) throws MetaClientException {
        super(metaClientConfig);

    }


    @Override
    public XAMessageProducer createXAProducer(final PartitionSelector partitionSelector) {
        if (partitionSelector == null) {
            throw new IllegalArgumentException("Null partitionSelector");
        }
        return this.addChild(new SimpleXAMessageProducer(this, this.remotingClient, partitionSelector,
            this.producerZooKeeper, this.sessionIdGenerator.generateId()));
    }


    @Override
    public XAMessageProducer createXAProducer() {
        return this.createXAProducer(new RoundRobinPartitionSelector());
    }

}

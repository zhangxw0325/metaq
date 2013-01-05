package com.taobao.metamorphosis.client.producer;

import javax.transaction.xa.XAResource;

import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 支持XA事务的消息生产者
 * 
 * @author boyan
 * 
 */
public interface XAMessageProducer extends MessageProducer {
    /**
     * 返回一个XAResource对象。事务管理器将使用该对象来管理XAMessageProducer参与到一个分布式事务中。
     * 
     * @return
     */
    public XAResource getXAResource() throws MetaClientException;
}

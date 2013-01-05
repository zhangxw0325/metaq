package com.taobao.metamorphosis.client.consumer;

import java.util.List;


/**
 * Consumer的balance策略
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-29
 * 
 */
public interface LoadBalanceStrategy {

    enum Type {
        DEFAULT,
        CONSIST
    }


    /**
     * 根据consumer id查找对应的分区列表
     * 
     * @param topic
     *            分区topic
     * @param consumerId
     *            consumerId
     * @param curConsumers
     *            当前所有的consumer列表
     * @param curPartitions
     *            当前的分区列表
     * 
     * @return
     */
    public List<String> getPartitions(String topic, String consumerId, final List<String> curConsumers,
            final List<String> curPartitions);
}

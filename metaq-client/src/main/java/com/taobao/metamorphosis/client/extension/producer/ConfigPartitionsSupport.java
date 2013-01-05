package com.taobao.metamorphosis.client.extension.producer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.taobao.metamorphosis.client.producer.PartitionSelector;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 支持获取某topic分区总数的Selector
 * 
 * @author 无花
 * @since 2011-8-2 下午02:49:27
 */
public abstract class ConfigPartitionsSupport implements PartitionSelector, ConfigPartitionsAware {

    private Map<String, List<Partition>> partitionsNumMap;


    @Override
    synchronized public void setConfigPartitions(final Map<String/* topic */, List<Partition>> map) {
        this.partitionsNumMap = map;
    }


    @Override
    public synchronized List<Partition> getConfigPartitions(final String topic) {
        final List<Partition> partitions = this.partitionsNumMap != null ? this.partitionsNumMap.get(topic) : null;
        return partitions != null ? partitions : (List<Partition>) Collections.EMPTY_LIST;
    }
}

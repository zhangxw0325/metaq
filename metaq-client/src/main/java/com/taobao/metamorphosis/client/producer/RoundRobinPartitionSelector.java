package com.taobao.metamorphosis.client.producer;

import java.util.List;

import com.taobao.gecko.core.util.PositiveAtomicCounter;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 轮询的分区选择器，默认使用此选择器
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public class RoundRobinPartitionSelector implements PartitionSelector {

    private final PositiveAtomicCounter sets = new PositiveAtomicCounter();


    @Override
    public Partition getPartition(final String topic, final List<Partition> partitions, final Message message)
            throws MetaClientException {
        if (partitions == null) {
            throw new MetaClientException("There is no aviable partition for topic " + topic
                    + ",maybe you don't publish it at first?");
        }
        try {
            return partitions.get(this.sets.incrementAndGet() % partitions.size());
        }
        catch (final Throwable t) {
            throw new MetaClientException(t);
        }
    }

}

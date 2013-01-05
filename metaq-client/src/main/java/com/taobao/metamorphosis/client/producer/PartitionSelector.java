package com.taobao.metamorphosis.client.producer;

import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 分区选择器
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public interface PartitionSelector {

    /**
     * 根据topic、message从partitions列表中选择分区
     * 
     * @param topic
     *            topic
     * @param partitions
     *            分区列表
     * @param message
     *            消息
     * @return
     * @throws MetaClientException
     *             此方法抛出的任何异常都应当包装为MetaClientException
     */
    public Partition getPartition(String topic, List<Partition> partitions, Message message) throws MetaClientException;
}

package com.taobao.metamorphosis.client.consumer.storage;

import java.util.Collection;

import com.taobao.metamorphosis.client.consumer.TopicPartitionRegInfo;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 基于Tair的offset保存期
 * 
 * @author boyan
 * @Date 2011-4-28
 * 
 */
// TODO
public class TairOffsetStorage implements OffsetStorage {

    public void commitOffset(String group, Collection<TopicPartitionRegInfo> infoList) {
        // TODO Auto-generated method stub

    }


    public TopicPartitionRegInfo load(String topic, String group, Partition partition) {
        // TODO Auto-generated method stub
        return null;
    }


    public void close() {
        // TODO Auto-generated method stub

    }


    public void initOffset(String topic, String group, Partition partition, long offset) {
        // TODO Auto-generated method stub

    }

}

package com.taobao.metamorphosis.client.extension.producer;

import java.util.List;
import java.util.Map;

import com.taobao.metamorphosis.cluster.Partition;


/**
 * 支持获取某topic预配置的分区分布情况
 * 
 * @author 无花
 * @since 2011-8-2 下午02:49:27
 */
public interface ConfigPartitionsAware {

    /**
     * 设置顺序消息配置的总体分区信息
     * */
    public void setConfigPartitions(Map<String/* topic */, List<Partition>/* partitions */> map);


    /**
     * 获取某个topic消息的总体分区信息
     * */
    public List<Partition> getConfigPartitions(String topic);
}

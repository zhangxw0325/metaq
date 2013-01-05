package com.taobao.metamorphosis.tools.query;

import java.util.List;

/**
 * offset的查询接口
 * 
 * @author pingwei
 */
public interface OffsetStorageQuery {

    /**
     * 提供offset的查询，具体实现确定不同的查询数据源
     * 
     * @param queryDO
     * @return
     */
    String getOffset(OffsetQueryDO queryDO);

    public List<String> getConsumerGroups();

    public List<String> getTopicsExistOffset(String group);

    public List<String> getPartitionsOf(String group, String topic);
}

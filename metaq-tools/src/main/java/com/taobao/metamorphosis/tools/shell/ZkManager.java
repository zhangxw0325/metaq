package com.taobao.metamorphosis.tools.shell;

import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.tools.query.Query;
import com.taobao.metamorphosis.utils.ZkUtils;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-8-24 ÏÂÎç2:00:29
 */

public class ZkManager {
    Query query;


    ZkManager(Query query) {
        this.query = query;
    }


    public void setOffset(String topic, String group, Partition partition, String srcOffset) throws Exception {
        String path = query.getOffsetPath(group, topic, partition);
        ZkUtils.updatePersistentPath(this.query.getZkClient(), path, srcOffset);
    }
}

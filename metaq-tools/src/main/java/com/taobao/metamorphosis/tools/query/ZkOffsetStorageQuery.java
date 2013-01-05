package com.taobao.metamorphosis.tools.query;

import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.tools.utils.StringUtil;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.ZkUtils;


/**
 * 从zk中查询client端的offset数据
 * 
 * @author pingwei
 */
public class ZkOffsetStorageQuery implements OffsetStorageQuery {

    ZkClient zkClient;
    final MetaZookeeper metaZookeeper;
    final String consumerBasePath;// = ZkUtils.consumersPath + "/";//
                                  // "/meta/consumers/";


    public ZkOffsetStorageQuery(ZkClient zkClient, MetaZookeeper metaZookeeper) {
        this.zkClient = zkClient;
        this.metaZookeeper = metaZookeeper;
        this.consumerBasePath = metaZookeeper.consumersPath + "/";
    }


    public String getOffset(OffsetQueryDO queryDO) {
        if (!this.check(queryDO)) {
            return null;
        }
        String path =
                this.consumerBasePath + queryDO.getGroup() + "/offsets/" + queryDO.getTopic() + "/"
                        + queryDO.getPartition();
        return ZkUtils.readDataMaybeNull(this.zkClient, path);
    }
    
    public static long parseOffsetAsLong(String offsetString){
    	if (StringUtils.isBlank(offsetString)) {
			return -1;
		}
    	
    	String[]tmp=StringUtils.splitByWholeSeparator(offsetString, "-");
    	
    	try {
			
    		if (tmp!=null&&tmp.length==1) {
    			return Long.parseLong(offsetString);
    		}else if((tmp!=null&&tmp.length==2)){
    			return Long.parseLong(tmp[1]);
    		}else{
    			return -1;
    		}
		} catch (NumberFormatException e) {
			return -1;
		}
    }

    public List<String> getConsumerGroups() {
        return this.zkClient.getChildren(metaZookeeper.consumersPath);
    }


    public List<String> getTopicsExistOffset(String group) {
        try {
            return this.zkClient.getChildren(this.consumerBasePath + group + "/offsets");
        }
        catch (ZkNoNodeException e) {
            return Collections.emptyList();
        }
    }


    public List<String> getPartitionsOf(String group, String topic) {
        return this.zkClient.getChildren(this.consumerBasePath + group + "/offsets/" + topic);
    }


    private boolean check(OffsetQueryDO queryDO) {
        if (queryDO == null) {
            return false;
        }
        if (StringUtil.empty(queryDO.getGroup())) {
            return false;
        }
        if (StringUtil.empty(queryDO.getTopic())) {
            return false;
        }
        if (StringUtil.empty(queryDO.getPartition())) {
            return false;
        }
        return true;
    }


	

}

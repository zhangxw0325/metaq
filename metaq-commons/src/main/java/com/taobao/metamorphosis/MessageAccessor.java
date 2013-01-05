package com.taobao.metamorphosis;

import com.taobao.metamorphosis.cluster.Partition;


/**
 * 为了访问message中包级别的方法提供的辅助类
 * 
 * @author boyan
 * @Date 2011-4-27
 * 
 */
public class MessageAccessor {
    public static void setId(Message message, long id) {
        message.setId(id);
    }


    public static void setMsgNewId(Message message, String msgNewId) {
        message.setMsgNewId(msgNewId);
    }


    public static void setFlag(Message message, int flag) {
        message.setFlag(flag);
    }


    public static int getFlag(Message message) {
        return message.getFlag();
    }


    public static void setPartition(Message message, Partition partition) {
        message.setPartition(partition);
    }
}

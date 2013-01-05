package com.taobao.metamorphosis.tools.query;

/**
 * offset²éÑ¯²ÎÊý
 * 
 * @author pingwei
 * @author wuhua
 */
public class OffsetQueryDO {
    String topic;
    String group;
    String partition;
    QueryType type;

    //add by wuhua
    static public enum QueryType {
        zk,
        mysql
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public QueryType getType() {
        return type;
    }

    public void setType(QueryType type) {
        this.type = type;
    }

    public OffsetQueryDO(String topic, String group, String partition, String type) {
        super();
        this.topic = topic;
        this.group = group;
        this.partition = partition;
        this.type = QueryType.valueOf(type);
    }

    @Override
    public String toString() {
        return "OffsetQueryDO [topic=" + topic + ", group=" + group + ", partition=" + partition + ", type=" + type
                + "]";
    }
    
    public static void main(String[] args) {
        System.out.println(QueryType.zk.toString());
    }

}

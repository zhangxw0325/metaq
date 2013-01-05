/**
 * $Id: MetaMessage.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.commons;

public class MetaMessage {
    // 消息主题
    private String topic;
    // 消息类型
    private String type;
    // 消息属性
    private String attribute;
    // 消息标志
    private int flag;
    // 消息体
    private byte[] body;


    public MetaMessage(final String topic, final String type, byte[] body) {
        this(topic, type, null, 0, body);
    }


    public MetaMessage() {
    }


    public MetaMessage(String topic, String type, String attribute, int flag, byte[] body) {
        this.topic = topic;
        this.type = type;
        this.attribute = attribute;
        this.flag = flag;
        this.body = body;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getType() {
        return type;
    }


    public void setType(String type) {
        this.type = type;
    }


    public String getAttribute() {
        return attribute;
    }


    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }


    public int getFlag() {
        return flag;
    }


    public void setFlag(int flag) {
        this.flag = flag;
    }


    public byte[] getBody() {
        return body;
    }


    public void setBody(byte[] body) {
        this.body = body;
    }
}

package com.taobao.metamorphosis.client.consumer;

import java.io.Serializable;


/**
 * 订阅关系对象
 * 
 * @author boyan
 * @Date 2011-5-18
 * 
 */
class Subscription implements Serializable {
    static final long serialVersionUID = -1L;
    private String topic;

    private int maxSize;

    private transient MessageListener messageListener;


    public Subscription(final String topic, final int maxSize, final MessageListener messageListener) {
        super();
        this.topic = topic;
        this.maxSize = maxSize;
        this.messageListener = messageListener;
    }


    public Subscription() {
        super();
    }


    public String getTopic() {
        return this.topic;
    }


    public void setTopic(final String topic) {
        this.topic = topic;
    }


    public int getMaxSize() {
        return this.maxSize;
    }


    public void setMaxSize(final int maxSize) {
        this.maxSize = maxSize;
    }


    public MessageListener getMessageListener() {
        return this.messageListener;
    }


    public void setMessageListener(final MessageListener messageListener) {
        this.messageListener = messageListener;
    }

}

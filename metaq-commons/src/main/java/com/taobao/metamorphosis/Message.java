package com.taobao.metamorphosis;

import java.io.Serializable;
import java.util.Arrays;

import com.taobao.metamorphosis.cluster.Partition;


/**
 * 一条Metamorphosis的消息，消息必须有topic和data，也可以有一个附加字符串属性
 * 
 * @author boyan
 * @Date 2011-4-19
 * @author wuhua
 * @Date 2011-6-26
 * 
 */
public class Message implements Serializable {
    static final long serialVersionUID = -1L;
    private long id;
    private String topic;
    private byte[] data;
    private String attribute;
    private int flag;
    private Partition partition;
    private Object arg;

    /**
     * 这个变量值不会传递到服务端，一般用作客户端消息发送前的上下文传递，
     * 如：消息选择器需要根据data决定，避免对data数组的解析，可以使用该字段
     * @return
     */
    public Object getArg() {
		return arg;
	}


	public void setArg(Object arg) {
		this.arg = arg;
	}


	// 2.0 add
    private String msgNewId;

    private transient long offset = 0;


    public long getOffset() {
        return offset;
    }


    public void setOffset(long offset) {
        this.offset = offset;
    }


    public void setId(final long id) {
        this.id = id;
    }


    int getFlag() {
        return this.flag;
    }


    void setFlag(final int flag) {
        this.flag = flag;
    }


    /**
     * 消息是否有属性
     * 
     * @return
     */
    public boolean hasAttribute() {
        return this.attribute != null;
    }


    /**
     * 此方法已经废弃，总是返回false。不排除未来某个版本中移除此方法，请不要再使用。
     * 
     * @return
     */
    @Deprecated
    public boolean isOrdered() {
        return false;
    }


    public Message(final String topic, final byte[] data) {
        super();
        this.topic = topic;
        this.data = data;
    }


    public Message(final String topic, final byte[] data, final String attribute) {
        super();
        this.topic = topic;
        this.data = data;
        this.attribute = attribute;
    }


    /**
     * 返回消息Id，只有在发送成功后返回的id才有效，否则返回0
     * 
     * @return
     */
    public long getId() {
        return this.id;
    }


    /**
     * 返回消息属性
     * 
     * @return
     */
    public String getAttribute() {
        return this.attribute;
    }


    /**
     * 设置消息属性，消息属性非必须，必须为字符串
     * 
     * @param attribute
     */
    public void setAttribute(final String attribute) {
        this.attribute = attribute;
    }


    /**
     * 设置消息topic
     * 
     * @param topic
     */
    public void setTopic(final String topic) {
        this.topic = topic;
    }


    /**
     * 设置消息payload
     * 
     * @param data
     */
    public void setData(final byte[] data) {
        this.data = data;
    }


    /**
     * 返回消息topic
     * 
     * @return
     */
    public String getTopic() {
        return this.topic;
    }


    /**
     * 返回消息payload
     * 
     * @return
     */
    public byte[] getData() {
        return this.data;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.attribute == null ? 0 : this.attribute.hashCode());
        result = prime * result + Arrays.hashCode(this.data);
        result = prime * result + (int) (this.id ^ this.id >>> 32);
        result = prime * result + (this.topic == null ? 0 : this.topic.hashCode());
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final Message other = (Message) obj;
        if (this.attribute == null) {
            if (other.attribute != null) {
                return false;
            }
        }
        else if (!this.attribute.equals(other.attribute)) {
            return false;
        }
        if (!Arrays.equals(this.data, other.data)) {
            return false;
        }
        if (this.id != other.id) {
            return false;
        }
        if (this.topic == null) {
            if (other.topic != null) {
                return false;
            }
        }
        else if (!this.topic.equals(other.topic)) {
            return false;
        }
        return true;
    }


    void setPartition(final Partition partition) {
        this.partition = partition;
    }


    /**
     * 发送成功后，返回消息所在的分区，发送失败则为null
     * 
     * @return
     */
    public Partition getPartition() {
        return this.partition;
    }


    public String getMsgNewId() {
        return msgNewId;
    }


    public void setMsgNewId(String msgNewId) {
        this.msgNewId = msgNewId;
    }

}

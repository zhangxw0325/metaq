/**
 * $Id: MetaMessageAnnotation.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.commons;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;


/**
 * 消息的附加属性，在服务器产生此对象<br>
 * Producer ----> Broker ----> Consumer<br>
 */
public class MetaMessageAnnotation {
    // 队列ID
    private int queueId;
    // 存储记录大小
    private int storeSize;
    // 队列偏移量
    private long queueOffset;
    // 消息标志位（由Meta产生，用户不能设置）
    private int sysFlag;
    // 消息在客户端创建时间戳
    private long bornTimestamp;
    // 消息来自哪里
    private SocketAddress bornHost;
    // 消息在服务器存储时间戳
    private long storeTimestamp;
    // 消息存储在哪个服务器
    private SocketAddress storeHost;
    // 消息ID
    private String msgId;
    // 消息对应的物理Offset
    private long physicOffset;
    // 消息体CRC
    private int bodyCRC;
    // 消息请求ID
    private long requestId;


    public MetaMessageAnnotation() {
    }


    public MetaMessageAnnotation(int queueId, long bornTimestamp, SocketAddress bornHost, long storeTimestamp,
            SocketAddress storeHost, String msgId) {
        this.queueId = queueId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.storeTimestamp = storeTimestamp;
        this.storeHost = storeHost;
        this.msgId = msgId;
    }


    /**
     * SocketAddress ----> ByteBuffer 转化成8个字节
     */
    public static ByteBuffer SocketAddress2ByteBuffer(SocketAddress socketAddress) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        byteBuffer.put(inetSocketAddress.getAddress().getAddress());
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.flip();
        return byteBuffer;
    }


    /**
     * 获取bornHost字节形式，8个字节 HOST + PORT
     */
    public ByteBuffer getBornHostBytes() {
        return SocketAddress2ByteBuffer(this.bornHost);
    }


    /**
     * 获取storehost字节形式，8个字节 HOST + PORT
     */
    public ByteBuffer getStoreHostBytes() {
        return SocketAddress2ByteBuffer(this.storeHost);
    }


    public int getQueueId() {
        return queueId;
    }


    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }


    public long getBornTimestamp() {
        return bornTimestamp;
    }


    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }


    public SocketAddress getBornHost() {
        return bornHost;
    }


    public String getBornHostString() {
        if (this.bornHost != null) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) this.bornHost;
            return inetSocketAddress.getAddress().getHostAddress();
        }

        return null;
    }


    public void setBornHost(SocketAddress bornHost) {
        this.bornHost = bornHost;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }


    public SocketAddress getStoreHost() {
        return storeHost;
    }


    public void setStoreHost(SocketAddress storeHost) {
        this.storeHost = storeHost;
    }


    public String getMsgId() {
        return msgId;
    }


    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }


    public int getSysFlag() {
        return sysFlag;
    }


    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }


    public int getBodyCRC() {
        return bodyCRC;
    }


    public void setBodyCRC(int bodyCRC) {
        this.bodyCRC = bodyCRC;
    }


    public long getQueueOffset() {
        return queueOffset;
    }


    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }


    public long getPhysicOffset() {
        return physicOffset;
    }


    public void setPhysicOffset(long physicOffset) {
        this.physicOffset = physicOffset;
    }


    public long getRequestId() {
        return requestId;
    }


    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }


    public int getStoreSize() {
        return storeSize;
    }


    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }
}

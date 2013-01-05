/**
 * $Id: AppendMessageCallback.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

import java.nio.ByteBuffer;

/**
 * 写入消息回调
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface AppendMessageCallback {

    /**
     * 序列化消息后，写入MapedByteBuffer
     * 
     * @param byteBuffer
     *            要写入的target
     * @param maxBlank
     *            要写入的target最大空白区
     * @param msg
     *            要写入的message
     * @return 写入多少字节
     */
    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
            final int maxBlank, final Object msg);
}

/**
 * $Id: MetaMessageDecoder.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.commons;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class MetaMessageDecoder {
    /**
     * 消息ID定长
     */
    public final static int MSG_ID_LENGTH = 4 + 8 + 8;

    /**
     * 存储记录各个字段位置
     */
    public final static int MessageMagicCodePostion = 4;
    public final static int MessageFlagPostion = 16;
    public final static int MessagePhysicOffsetPostion = 28;
    public final static int MessageStoreTimestampPostion = 56;

    /**
     * 标记服务器版本为新版本
     */
    public final static int NewServerFlag = (1 << 31);


    public static String createMessageId(final ByteBuffer input, final int time, final ByteBuffer addr,
            final long offset) {
        input.flip();
        input.limit(MetaMessageDecoder.MSG_ID_LENGTH);

        // 消息存储时间 4
        input.putInt(time);
        // 消息存储主机地址 IP PORT 8
        input.put(addr);
        // 消息对应的物理分区 OFFSET 8
        input.putLong(offset);

        return MetaUtil.bytes2string(input.array());
    }


    public static MetaMessageWrapper decode(java.nio.ByteBuffer byteBuffer) {
        return decode(byteBuffer, true);
    }


    /**
     * 客户端使用，SLAVE也会使用
     */
    public static MetaMessageWrapper decode(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        MetaMessageWrapper wrapper = null;
        try {
            MetaMessageAnnotation msgant = new MetaMessageAnnotation();
            MetaMessage msg = new MetaMessage();

            // 1 TOTALSIZE
            int storeSize = byteBuffer.getInt();
            msgant.setStoreSize(storeSize);

            // 2 MAGICCODE
            byteBuffer.getInt();

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();
            msgant.setBodyCRC(bodyCRC);

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();
            msgant.setQueueId(queueId);

            // 5 FLAG
            int flag = byteBuffer.getInt();
            msg.setFlag(flag);

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();
            msgant.setQueueOffset(queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();
            msgant.setPhysicOffset(physicOffset);

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();
            msgant.setSysFlag(sysFlag);

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            msgant.setBornTimestamp(bornTimeStamp);

            // 10 BORNHOST
            byte[] bornHost = new byte[4];
            byteBuffer.get(bornHost, 0, 4);
            int port = byteBuffer.getInt();
            msgant.setBornHost(new InetSocketAddress(InetAddress.getByAddress(bornHost), port));

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();
            msgant.setStoreTimestamp(storeTimestamp);

            // 12 STOREHOST
            byte[] storeHost = new byte[4];
            byteBuffer.get(storeHost, 0, 4);
            port = byteBuffer.getInt();
            msgant.setStoreHost(new InetSocketAddress(InetAddress.getByAddress(storeHost), port));

            // 13 REQUESTID
            long requestId = byteBuffer.getLong();
            msgant.setRequestId(requestId);

            // 14 TOPIC
            byte topicLen = byteBuffer.get();
            byte[] topic = new byte[(int) topicLen];
            byteBuffer.get(topic);
            msg.setTopic(new String(topic));

            // 15 TYPE
            byte typeLen = byteBuffer.get();
            byte[] type = new byte[(int) typeLen];
            byteBuffer.get(type);
            msg.setType(new String(type));

            // 16 ATTRIBUTE
            short attributeLen = byteBuffer.getShort();
            if (attributeLen > 0) {
                byte[] attribute = new byte[attributeLen];
                byteBuffer.get(attribute);
                msg.setAttribute(new String(attribute));
            }

            // 17 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byte[] body = new byte[bodyLen];
                    byteBuffer.get(body);

                    // uncompress body
                    if ((flag & 0x2) == 2) {
                        body = MetaUtil.uncompress(body);
                    }

                    msg.setBody(body);
                }
                else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 消息ID
            ByteBuffer byteBufferMsgId = ByteBuffer.allocate(MSG_ID_LENGTH);
            String msgId =
                    createMessageId(byteBufferMsgId, (int) (msgant.getStoreTimestamp() / 1000),
                        msgant.getStoreHostBytes(), msgant.getPhysicOffset());
            msgant.setMsgId(msgId);
            wrapper = new MetaMessageWrapper(msg, msgant);
        }
        catch (UnknownHostException e) {
            byteBuffer.position(byteBuffer.limit());
        }
        catch (BufferUnderflowException e) {
            byteBuffer.position(byteBuffer.limit());
        }
        catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return wrapper;
    }


    public static List<MetaMessageWrapper> decodes(java.nio.ByteBuffer byteBuffer) {
        return decodes(byteBuffer, true);
    }


    /**
     * 客户端使用
     */
    public static List<MetaMessageWrapper> decodes(java.nio.ByteBuffer byteBuffer, final boolean readBody) {
        List<MetaMessageWrapper> wrps = new ArrayList<MetaMessageWrapper>();
        while (byteBuffer.hasRemaining()) {
            MetaMessageWrapper metaMessageWrapper = decode(byteBuffer, readBody);
            if (null != metaMessageWrapper) {
                wrps.add(metaMessageWrapper);
            }
            else {
            	log.warn("message decode error.");
                break;
            }
        }
        return wrps;
    }
    
    private static final Log log = LogFactory.getLog(MetaMessageDecoder.class);
}

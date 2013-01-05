/**
 * $Id: RecoverTest.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metaq.store;

import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.metaq.commons.MetaMessage;
import com.taobao.metaq.commons.MetaMessageAnnotation;
import com.taobao.metaq.commons.MetaMessageDecoder;
import com.taobao.metaq.commons.MetaMessageWrapper;


public class RecoverTest {
    // 队列个数
    private static int QUEUE_TOTAL = 10;
    // 发往哪个队列
    private static AtomicInteger QueueId = new AtomicInteger(0);
    // 发送主机地址
    private static SocketAddress BornHost;
    // 存储主机地址
    private static SocketAddress StoreHost;
    // 消息体
    private static byte[] MessageBody;

    private static final String StoreMessage = "Once, there was a chance for me!aaaaaaaaaaaaaaaaaaaaaaaa";


    public static MetaMessageWrapper buildMessage() {
        MetaMessage msg = new MetaMessage("TOPIC_A", "MSG_TYPE_A", MessageBody);
        msg.setAttribute("");

        MetaMessageAnnotation msgant = new MetaMessageAnnotation();
        msgant.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msgant.setSysFlag(4);
        msgant.setBornTimestamp(System.currentTimeMillis());
        msgant.setStoreHost(StoreHost);
        msgant.setBornHost(BornHost);

        return new MetaMessageWrapper(msg, msgant);
    }


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("10.232.102.184"), 0);
        PropertyConfigurator.configure("log4j.properties");
    }


    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    private MetaStore metaStoreWrite1;
    private MetaStore metaStoreWrite2;
    private MetaStore metaStoreRead;


    private void destroy() {
        if (metaStoreWrite1 != null) {
            // 关闭存储服务
            metaStoreWrite1.shutdown();
            // 删除文件
            metaStoreWrite1.destroy();
        }

        if (metaStoreWrite2 != null) {
            // 关闭存储服务
            metaStoreWrite2.shutdown();
            // 删除文件
            metaStoreWrite2.destroy();
        }

        if (metaStoreRead != null) {
            // 关闭存储服务
            metaStoreRead.shutdown();
            // 删除文件
            metaStoreRead.destroy();
        }
    }


    public void writeMessage(boolean normal, boolean first) throws Exception {
        System.out.println("================================================================");
        long totalMsgs = 1000;
        QUEUE_TOTAL = 3;

        // 构造消息体
        MessageBody = StoreMessage.getBytes();

        MetaStoreConfig metaStoreConfig = new MetaStoreConfig();
        // 每个物理映射文件
        metaStoreConfig.setMapedFileSizePhysic(1024 * 32);
        // 每个逻辑映射文件
        metaStoreConfig.setMapedFileSizeLogics(1024);

        MetaStore metaStore = new DefaultMetaStore(metaStoreConfig);
        if (first) {
            this.metaStoreWrite1 = metaStore;
        }
        else {
            this.metaStoreWrite2 = metaStore;
        }

        // 第一步，load已有数据
        boolean loadResult = metaStore.load();
        assertTrue(loadResult);

        // 第二步，启动服务
        metaStore.start();

        // 第三步，发消息
        for (long i = 0; i < totalMsgs; i++) {
            MetaMessageWrapper wrapper = buildMessage();
            PutMessageResult result =
                    metaStore.putMessage(wrapper.getMetaMessage(), wrapper.getMetaMessageAnnotation());

            System.out.println(i + "\t" + result.getAppendMessageResult().getMsgId());
        }

        if (normal) {
            // 关闭存储服务
            metaStore.shutdown();
        }

        System.out.println("========================writeMessage OK========================================");
    }


    private void veryReadMessage(int queueId, long queueOffset, List<ByteBuffer> byteBuffers) {
        for (ByteBuffer byteBuffer : byteBuffers) {
            MetaMessageWrapper msg = MetaMessageDecoder.decode(byteBuffer);
            System.out.println("request queueId " + queueId + ", request queueOffset " + queueOffset
                    + " msg queue offset " + msg.getMetaMessageAnnotation().getQueueOffset());

            assertTrue(msg.getMetaMessageAnnotation().getQueueOffset() == queueOffset);

            queueOffset++;
        }
    }


    public void readMessage(final long msgCnt) throws Exception {
        System.out.println("================================================================");
        QUEUE_TOTAL = 3;

        // 构造消息体
        MessageBody = StoreMessage.getBytes();

        MetaStoreConfig metaStoreConfig = new MetaStoreConfig();
        // 每个物理映射文件
        metaStoreConfig.setMapedFileSizePhysic(1024 * 32);
        // 每个逻辑映射文件
        metaStoreConfig.setMapedFileSizeLogics(1024);

        metaStoreRead = new DefaultMetaStore(metaStoreConfig);
        // 第一步，load已有数据
        boolean loadResult = metaStoreRead.load();
        assertTrue(loadResult);

        // 第二步，启动服务
        metaStoreRead.start();

        // 第三步，收消息
        long readCnt = 0;
        for (int queueId = 0; queueId < QUEUE_TOTAL; queueId++) {
            for (long offset = 0;;) {
                GetMessageResult result = metaStoreRead.getMessage("TOPIC_A", queueId, offset, 1024 * 1024, null);
                if (result.getStatus() == GetMessageStatus.FOUND) {
                    System.out.println(queueId + "\t" + result.getMessageCount());
                    this.veryReadMessage(queueId, offset, result.getMessageBufferList());
                    offset += result.getMessageCount();
                    readCnt += result.getMessageCount();
                    result.release();
                }
                else {
                    break;
                }
            }
        }

        System.out.println("readCnt = " + readCnt);
        assertTrue(readCnt == msgCnt);

        System.out.println("========================readMessage OK========================================");
    }


    /**
     * 正常关闭后，重启恢复消息，验证是否有消息丢失
     */
    @Test
    public void test_recover_normally() throws Exception {
        this.writeMessage(true, true);
        Thread.sleep(1000 * 3);
        this.readMessage(1000);
        this.destroy();
    }


    /**
     * 正常关闭后，重启恢复消息，并再次写入消息，验证是否有消息丢失
     */
    @Test
    public void test_recover_normally_write() throws Exception {
        this.writeMessage(true, true);
        Thread.sleep(1000 * 3);
        this.writeMessage(true, false);
        Thread.sleep(1000 * 3);
        this.readMessage(2000);
        this.destroy();
    }


    /**
     * 异常关闭后，重启恢复消息，验证是否有消息丢失
     */
    @Test
    public void test_recover_abnormally() throws Exception {
        this.writeMessage(false, true);
        Thread.sleep(1000 * 3);
        this.readMessage(1000);
        this.destroy();
    }


    /**
     * 异常关闭后，重启恢复消息，并再次写入消息，验证是否有消息丢失
     */
    @Test
    public void test_recover_abnormally_write() throws Exception {
        this.writeMessage(false, true);
        Thread.sleep(1000 * 3);
        this.writeMessage(false, false);
        Thread.sleep(1000 * 3);
        this.readMessage(2000);
        this.destroy();
    }
}

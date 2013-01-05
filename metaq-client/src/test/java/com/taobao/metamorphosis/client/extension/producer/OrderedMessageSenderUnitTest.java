package com.taobao.metamorphosis.client.extension.producer;

import java.util.concurrent.TimeUnit;

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.cluster.Partition;


/**
 * 
 * @author 无花
 * @since 2011-8-8 上午11:21:40
 */

public class OrderedMessageSenderUnitTest {

    private OrderedMessageProducer producer;
    private IMocksControl mocksControl;
    private OrderedMessageSender sender;
    private final Partition partition = new Partition("0-0");


    @Before
    public void setUp() {
        this.mocksControl = EasyMock.createControl();
        this.producer = this.mocksControl.createMock(OrderedMessageProducer.class);
        this.sender = new OrderedMessageSender(this.producer);
    }


    @Test
    public void testSendMessage_PartitionNumWrong() throws Exception {
        // 检测到分区不可用,本次消息存储到本地

        final Message message = this.createDefaultMessage();
        // EasyMock.expect(this.producer.getLocalMessageCount(message.getTopic(),
        // this.partition)).andReturn(2);
        EasyMock.expect(this.producer.selectPartition(message)).andThrow(
            new AvailablePartitionNumException("xx[0-0]xx"));
        EasyMock.expect(this.producer.saveMessageToLocal(message, this.partition, 10000, TimeUnit.MILLISECONDS))
            .andReturn(null);
        this.mocksControl.replay();
        this.sender.sendMessage(message, 10000, TimeUnit.MILLISECONDS);
        this.mocksControl.verify();
    }


    @Test
    public void testSendMessage_swichToNomal() throws Exception {
        final Message message = this.createDefaultMessage();
        // 分区已经正常,本地缓存的消息条数为0，切换为正常发送模式，并把本次消息写到服务端

        EasyMock.expect(this.producer.getLocalMessageCount(message.getTopic(), this.partition)).andReturn(0);
        EasyMock.expect(this.producer.selectPartition(message)).andReturn(new Partition("0-0"));
        EasyMock.expect(this.producer.sendMessageToServer(message, 10000, TimeUnit.MILLISECONDS, true)).andReturn(null);

        this.mocksControl.replay();
        this.sender.sendMessage(message, 10000, TimeUnit.MILLISECONDS);
        this.mocksControl.verify();
    }


    @Test
    public void testSendMessage_PartitionNumRight_butHaveFewLocalMessage() throws Exception {
        // 分区已经正常,本地还缓存了少量消息，停顿一下再检测,超过n次后还进入这条路径的话,本次消息写本地

        final Message message = this.createDefaultMessage();
        EasyMock.expect(this.producer.getLocalMessageCount(message.getTopic(), this.partition)).andReturn(10).times(3);
        EasyMock.expect(this.producer.selectPartition(message)).andReturn(new Partition("0-0")).times(3);
        EasyMock.expect(this.producer.saveMessageToLocal(message, this.partition, 10000, TimeUnit.MILLISECONDS))
            .andReturn(null);
        this.producer.tryRecoverMessage(message.getTopic(), this.partition);
        EasyMock.expectLastCall().times(3);
        this.mocksControl.replay();
        this.sender.sendMessage(message, 10000, TimeUnit.MILLISECONDS);
        this.mocksControl.verify();
    }


    @Test
    public void testSendMessage_PartitionNumRight_butHaveFewLocalMessage2() throws Exception {
        // 分区已经正常,本地还缓存了少量消息，停顿一下再检测,第二次检测到本地消息恢复完毕,本次消息写服务器,切换到正常发送模式

        final Message message = this.createDefaultMessage();
        EasyMock.expect(this.producer.getLocalMessageCount(message.getTopic(), this.partition)).andReturn(10);
        EasyMock.expect(this.producer.getLocalMessageCount(message.getTopic(), this.partition)).andReturn(0);
        EasyMock.expect(this.producer.selectPartition(message)).andReturn(new Partition("0-0")).times(2);
        EasyMock.expect(this.producer.sendMessageToServer(message, 10000, TimeUnit.MILLISECONDS, true)).andReturn(null);
        this.producer.tryRecoverMessage(message.getTopic(), this.partition);
        EasyMock.expectLastCall().times(1);
        this.mocksControl.replay();
        this.sender.sendMessage(message, 10000, TimeUnit.MILLISECONDS);
        this.mocksControl.verify();
    }


    @Test
    public void testSendMessage_PartitionNumRight_butHaveManyLocalMessage() throws Exception {

    }


    private Message createDefaultMessage() {
        final String topic = "topic1";
        final byte[] data = "hello".getBytes();
        final Message message = new Message(topic, data);
        return message;
    }

}

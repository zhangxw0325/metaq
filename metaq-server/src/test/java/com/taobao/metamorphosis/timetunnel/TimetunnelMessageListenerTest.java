//package com.taobao.metamorphosis.timetunnel;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//
//import org.easymock.classextension.EasyMock;
//import org.junit.Before;
//import org.junit.Test;
//
//import com.taobao.metamorphosis.network.PutCommand;
//import com.taobao.metamorphosis.server.CommandProcessor;
//import com.taobao.timetunnel2.Message;
//import com.taobao.timetunnel2.MessageImpl;
//
//
///**
// * @author ÎÞ»¨
// * @since 2011-6-8 ÉÏÎç10:17:45
// */
//public class TimetunnelMessageListenerTest {
//
//    private TimetunnelMessageListener timetunnelMessageListener;
//    private CommandProcessor commandProcessor;
//
//
//    @Before
//    public void setUp() {
//        this.commandProcessor = EasyMock.createMock(CommandProcessor.class);
//        this.timetunnelMessageListener = new TimetunnelMessageListener(this.commandProcessor);
//    }
//
//
//    @Test
//    public void testReceiveSuccess() throws Exception {
//        final List<Message> messages = new ArrayList<Message>();
//        final Message message1 =
//                new MessageImpl("topic1", "idxxx", "testcontent1".getBytes(), "10.10.1.1", System.currentTimeMillis(),
//                    Collections.EMPTY_MAP) {
//                };
//        // Message message2 = new MessageImpl("topic2", "idxxxx",
//        // "testcontent2".getBytes(), "10.10.1.2", System
//        // .currentTimeMillis(), Collections.EMPTY_MAP) {
//        // };
//        messages.add(message1);
//        // messages.add(message2);
//
//        final byte[] data = TimetunnelMessageListener.getData(message1);
//        this.commandProcessor.processPutCommand(new PutCommand("topic1", -1, data, null, 0, 0), null, null);
//        EasyMock.expectLastCall();
//        EasyMock.replay(this.commandProcessor);
//
//        this.timetunnelMessageListener.receive(null, messages);
//
//        EasyMock.verify(this.commandProcessor);
//
//    }
//
//
//    @Test
//    public void testReceiveNoMessageReceived() throws Exception {
//        this.timetunnelMessageListener.receive(null, Collections.EMPTY_LIST);
//        this.timetunnelMessageListener.receive(null, null);
//    }
//
//
//    @Test
//    public void testReceiveThrowException() throws Exception {
//        final List<Message> messages = new ArrayList<Message>();
//        final Message message1 =
//                new MessageImpl("topic1", "idxxx", "testcontent1".getBytes(), "10.10.1.1", System.currentTimeMillis(),
//                    Collections.EMPTY_MAP) {
//                };
//        messages.add(message1);
//
//        final byte[] data = TimetunnelMessageListener.getData(message1);
//        this.commandProcessor.processPutCommand(new PutCommand("topic1", -1, data, null, 0, 0), null, null);
//        EasyMock.expectLastCall().andThrow(new RuntimeException());
//
//        EasyMock.replay(this.commandProcessor);
//
//        this.timetunnelMessageListener.receive(null, messages);
//
//        EasyMock.verify(this.commandProcessor);
//
//    }
//}

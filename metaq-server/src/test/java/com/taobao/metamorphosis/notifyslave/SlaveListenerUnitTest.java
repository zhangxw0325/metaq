package com.taobao.metamorphosis.notifyslave;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.easymock.IAnswer;
import org.easymock.classextension.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.AppendMessageErrorException;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.notifyslave.SlaveListener.PutOp;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.notify.codec.impl.JavaSerializer;
import com.taobao.notify.message.StringMessage;
import com.taobao.notify.remotingclient.MessageStatus;


public class SlaveListenerUnitTest {
    private SlaveListener slaveListener;

    private CommandProcessor commandProcessor;


    @Before
    public void setUp() {
        this.commandProcessor = EasyMock.createMock(CommandProcessor.class);
        this.slaveListener = new SlaveListener(this.commandProcessor);
    }


    @Test
    public void testReceiveMessageAppendSuccess() throws Exception {
        final StringMessage message = new StringMessage();
        message.setTopic("topic1");
        message.setMessageType("msgType1");
        message.setBody("hello world");
        final String newTopic = "topic1-msgType1";
        final byte[] data = new JavaSerializer().encodeObject(message);
        final PutOp op = new PutOp();
        this.commandProcessor.processPutCommand(new PutCommand(newTopic, -1, data, null, 0, 0), null, op);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((PutOp) EasyMock.getCurrentArguments()[2])
                    .putComplete(new BooleanCommand(0, HttpStatus.Success, null));
                return null;
            }

        });
        EasyMock.replay(this.commandProcessor);
        final MessageStatus status = new MessageStatus();
        this.slaveListener.receiveMessage(message, status);
        EasyMock.verify(this.commandProcessor);
        assertFalse(status.isRollbackOnly());

    }


    @Test
    public void testReceiveMessageAppendFailed() throws Exception {
        final StringMessage message = new StringMessage();
        message.setTopic("topic1");
        message.setMessageType("msgType1");
        message.setBody("hello world");
        final String newTopic = "topic1-msgType1";
        final byte[] data = new JavaSerializer().encodeObject(message);
        final PutOp op = new PutOp();
        this.commandProcessor.processPutCommand(new PutCommand(newTopic, -1, data, null, 0, 0), null, op);
        EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                ((PutOp) EasyMock.getCurrentArguments()[2]).putComplete(new BooleanCommand(0,
                    HttpStatus.InternalServerError, "just a test"));
                return null;
            }

        });
        EasyMock.replay(this.commandProcessor);
        final MessageStatus status = new MessageStatus();
        this.slaveListener.receiveMessage(message, status);

        EasyMock.verify(this.commandProcessor);
        assertTrue(status.isRollbackOnly());

    }


    @Test
    public void testReceiveMessageAppendThrowException() throws Exception {
        final StringMessage message = new StringMessage();
        message.setTopic("topic1");
        message.setMessageType("msgType1");
        message.setBody("hello world");
        final String newTopic = "topic1-msgType1";
        final byte[] data = new JavaSerializer().encodeObject(message);
        final PutOp op = new PutOp();
        this.commandProcessor.processPutCommand(new PutCommand(newTopic, -1, data, null, 0, 0), null, op);
        EasyMock.expectLastCall().andThrow(new RuntimeException());

        EasyMock.replay(this.commandProcessor);
        final MessageStatus status = new MessageStatus();
        try {
            this.slaveListener.receiveMessage(message, status);
            fail();
        }
        catch (final AppendMessageErrorException e) {

        }
        EasyMock.verify(this.commandProcessor);
        assertFalse(status.isRollbackOnly());

    }
}

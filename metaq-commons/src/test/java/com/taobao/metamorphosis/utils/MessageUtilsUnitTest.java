package com.taobao.metamorphosis.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.utils.MessageUtils.DecodedMessage;


public class MessageUtilsUnitTest {
    @Test
    public void testMakeBufferDecode() throws Exception {
        final long msgId = 10000L;
        final String topic = "test";
        final PutCommand req = new PutCommand(topic, 1, "hello".getBytes(), null, 0, -1);
        final ByteBuffer buf = MessageUtils.makeMessageBuffer(msgId, req);

        final DecodedMessage decodedMsg = MessageUtils.decodeMessage(topic, buf.array(), 0);
        assertNotNull(decodedMsg);
        assertEquals(topic, decodedMsg.message.getTopic());
        assertNull(decodedMsg.message.getAttribute());
        assertEquals(msgId, decodedMsg.message.getId());
        assertEquals("hello", new String(decodedMsg.message.getData()));
    }
}

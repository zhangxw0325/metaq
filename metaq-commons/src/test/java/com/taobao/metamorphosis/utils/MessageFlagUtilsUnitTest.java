package com.taobao.metamorphosis.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.taobao.metamorphosis.Message;


public class MessageFlagUtilsUnitTest {
    @Test
    public void testFlagWithoutAttribute() {
        final Message message = new Message("test", "hello".getBytes());
        final int flag = MessageFlagUtils.getFlag(message);

        assertFalse(MessageFlagUtils.hasAttribute(flag));

    }


    @Test
    public void testFlagWithAttribute() {
        final Message message = new Message("test", "hello".getBytes(), "");
        final int flag = MessageFlagUtils.getFlag(message);

        assertTrue(MessageFlagUtils.hasAttribute(flag));

    }
}

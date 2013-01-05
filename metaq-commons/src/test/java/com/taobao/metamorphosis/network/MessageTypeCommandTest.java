package com.taobao.metamorphosis.network;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class MessageTypeCommandTest {
	@Test
	public void test_encode_消息类型为空(){
		String topic = "topic";
		String group = "group";
		String version = "2.1";
		int opaque = 2012;
		Set<String> messageTypes = new HashSet<String>();
		long time = System.currentTimeMillis();
		MessageTypeCommand mtc = new MessageTypeCommand(version, group, topic, opaque, messageTypes, time);
		String result = "messageType 2.1 group topic 2012 "+ time+" 1\r\n*";
		Assert.assertEquals(result, new String(mtc.encode().array()));
	}
	
	
	@Test
	public void test_encode_消息类型不空(){
		String topic = "topic";
		String group = "group";
		String version = "2.1";
		int opaque = 2012;
		Set<String> messageTypes = new HashSet<String>();
		messageTypes.add("type1 a");
		messageTypes.add("type2");
		long time = System.currentTimeMillis();
		MessageTypeCommand mtc = new MessageTypeCommand(version, group, topic, opaque, messageTypes, time);
		String result = "messageType 2.1 group topic 2012 "+ time+" 13\r\n" + StringUtils.join(messageTypes, MessageTypeCommand.SEP);
		System.out.println(result);
		Assert.assertEquals(result, new String(mtc.encode().array()));
	}
}

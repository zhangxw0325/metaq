package com.taobao.metamorphosis.network;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.taobao.gecko.core.buffer.IoBuffer;

public class FetchCommandTest {
	
	
	
	@Test
	public void test_encode(){
		String version = "2.1";
		String topic = "test";
		String group = "pingwei-test";
		int partition = 2;
		long offset = 1000;
		int maxSize = 1024;
		int opaque = 1023;
		long time = System.currentTimeMillis();
		FetchCommand fc = new FetchCommand(version, topic, group, partition, offset, maxSize, opaque, time);
		IoBuffer buf = fc.encode();
		assertEquals(0, buf.position());
		assertEquals("fetch 2.1 test pingwei-test 2 1000 1024 1023 " + time +"\r\n", new String(buf.array()));
	}
	
}

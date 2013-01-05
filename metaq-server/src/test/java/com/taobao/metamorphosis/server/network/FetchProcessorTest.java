package com.taobao.metamorphosis.server.network;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.FetchCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metaq.store.GetMessageResult;
import com.taobao.metaq.store.GetMessageStatus;

public class FetchProcessorTest extends BaseProcessorUnitTest {
	private FetchProcessor fetchProcessor;
	String group = "pingwei_group";
	String topic = "pingwei_topic";
	String version = "2.1";
	int partition = 1;
	long offset = 1000;
	int maxSize = 1024;
	int opaque = 213;
	long clientStartTime = System.currentTimeMillis();

	@Before
	public void setup() {
		super.mock();
		fetchProcessor = new FetchProcessor(commandProcessor, null);
	}

	@Test
	public void test_第一次请求需要客户端汇报消息过滤信息() {
		EasyMock.expect(this.messageTypeManager.getMessageType(group, topic, clientStartTime)).andReturn(null);
		RemotingUtils.response(conn, new BooleanCommand(this.opaque, HttpStatus.Continue, "messageType not found."));
		this.mocksControl.replay();
		FetchCommand fetchCommand = new FetchCommand(version, topic, group, partition, offset, maxSize, opaque,
				clientStartTime);
		this.fetchProcessor.handleRequest(fetchCommand, conn);
		this.mocksControl.verify();
	}

	@Test
	public void test_已经有客户端消息过滤信息() throws Exception{
		Set<Integer> hashList = new HashSet<Integer>();
		hashList.add("*".hashCode());
		EasyMock.expect(this.messageTypeManager.getMessageType(group, topic, clientStartTime)).andReturn(
				new HashSet<String>());
		EasyMock.expect(this.messageTypeManager.getMessageTypeHash(group, topic)).andReturn(hashList);
		GetMessageResult getResult = this.mocksControl.createMock(GetMessageResult.class);
		EasyMock.expect(getResult.getStatus()).andReturn(GetMessageStatus.NO_MATCHED_MESSAGE);
		EasyMock.expect(getResult.getNextBeginOffset()).andReturn(1000L);
		EasyMock.expect(this.metaStore.getMessage(topic, partition, offset, maxSize, hashList)).andReturn(getResult);
		conn.response(new BooleanCommand(opaque, HttpStatus.Moved, "1000"));
		this.mocksControl.replay();
		FetchCommand fetchCommand = new FetchCommand(version, topic, group, partition, offset, maxSize, opaque,
				clientStartTime);
		this.fetchProcessor.handleRequest(fetchCommand, conn);
		this.mocksControl.verify();
	}
}

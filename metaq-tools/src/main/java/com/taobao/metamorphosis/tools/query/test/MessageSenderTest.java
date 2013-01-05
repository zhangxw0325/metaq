package com.taobao.metamorphosis.tools.query.test;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class MessageSenderTest {
	public static void main(String[] args) throws Exception {
		MetaClientConfig config = new MetaClientConfig();
		ZKConfig zkConfig = new ZKConfig("10.249.197.121", 30000, 30000, 5000);
		config.setZkConfig(zkConfig);
		MetaMessageSessionFactory factory = new MetaMessageSessionFactory(config);
		MessageProducer producer = factory.createProducer(false);
		String topic = "test";
		producer.publish(topic);
		Message message = new Message(topic, new byte[128]);
		producer.sendMessage(message);
		
	}
	
}

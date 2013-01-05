package com.taobao.metamorphosis.tools.query.test;

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class MessageConsumerTest {
	public static void main(String[] args) throws Exception{
		MetaClientConfig config = new MetaClientConfig();
		ZKConfig zkConfig = new ZKConfig("10.249.197.121", 30000, 30000, 5000);
		config.setZkConfig(zkConfig);
		MetaMessageSessionFactory factory = new MetaMessageSessionFactory(config);
		String topic = "test";
		String group = "pingwei";
		ConsumerConfig consumerConfig = new ConsumerConfig(group);
		MessageConsumer consumer = factory.createConsumer(consumerConfig);
		consumer.subscribe(topic, 1024*1024, new MessageListener() {
			
			public void recieveMessages(Message message) {
				System.out.println("receive message " + new String(message.getData()));
			}
			
			public Executor getExecutor() {
				return null;
			}
		});
		consumer.completeSubscribe();
	}
}

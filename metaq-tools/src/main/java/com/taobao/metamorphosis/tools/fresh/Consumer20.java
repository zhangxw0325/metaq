package com.taobao.metamorphosis.tools.fresh;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


public class Consumer20 
{
	public static final int Threshold=10000*20;
	public static void main(String[] args) throws Exception
	{
		final AtomicInteger count=new AtomicInteger(0);
		final long start=System.currentTimeMillis();
		
		if(2<args.length)
		{
			System.out.println("need topic group");
			System.exit(1);
		}
		final String topic = args[0];
		final String group = args[1];
		
		ZKConfig zkConfig=new ZKConfig();
		zkConfig.zkConnect="10.232.24.33:2181";
		
		MetaClientConfig metaClientConfig=new MetaClientConfig();
		metaClientConfig.setZkConfig(zkConfig);
		
		MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
		
		ConsumerConfig consumerConfig=new ConsumerConfig(group);
		//consumerConfig.setConsumeFromMaxOffset();
		
		MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);
		
		consumer.subscribe(topic, 1024 * 1024, new MessageListener() 
		{
            public void recieveMessages(Message message) 
            {
            	if((count.incrementAndGet() % Threshold)==0)
            	{
            		long end=System.currentTimeMillis();
            		System.out.println("cost:"+(end-start)+",tps:"+((count.get()*1000)/(end-start)));
            	}
            }
            public Executor getExecutor() 
            {
                return null;
            }
        });
		
        consumer.completeSubscribe();
		System.out.println("comsumer ok");
	}

}

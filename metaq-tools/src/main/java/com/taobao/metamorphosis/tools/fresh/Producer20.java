package com.taobao.metamorphosis.tools.fresh;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Random;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class Producer20 
{
	public static void main(String[] args) throws Exception
	{
		ZKConfig zkConfig=new ZKConfig();
		zkConfig.zkConnect="10.232.20.133:2181";
		
		MetaClientConfig metaClientConfig=new MetaClientConfig();
		metaClientConfig.setZkConfig(zkConfig);
		metaClientConfig.setCompressMessage(true);
		metaClientConfig.setCompressLevel(9);
		
        MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        MessageProducer producer = sessionFactory.createProducer();
        final String topic = args[0];
        producer.publish(topic);

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = null;
        System.out.println("start");
        while(true)
        {
        	int i=0;
	        while (i<10)
	        //while ((line = reader.readLine()) != null)
	        {
	        	try
	        	{
	        		//line=i+"";
	        		line=System.currentTimeMillis()+"";
	        		i++;
		            SendResult sendResult = producer.sendMessage(new Message(topic, line.getBytes()));
		            if (!sendResult.isSuccess()) 
		            {
		                System.err.println("Send message failed,error message:" + sendResult.getErrorMessage());
		            }
		            else 
		            {
		                System.out.println("Send message successfully,sent to " + sendResult.getPartition());
		                System.out.println("timestamp:"+line);
		            }
	        	}
	        	catch(Exception e )
	        	{
	        		e.printStackTrace();
	        	}
	        }
	        
	        Thread.sleep(10);
        }
	}
}

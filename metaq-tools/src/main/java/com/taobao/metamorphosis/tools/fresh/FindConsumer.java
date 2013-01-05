package com.taobao.metamorphosis.tools.fresh;

import java.text.MessageFormat;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;

import com.taobao.metamorphosis.utils.ZkUtils;

/**
 * 查询topic有哪些消费端
 */
public class FindConsumer extends Routine
{
	public static final String CONSUMER="/meta/consumers";
	//0:group;1:topic
	public static final MessageFormat consumerPartition=new MessageFormat("/meta/consumers/{0}/owners/{1}");
	//0:group
	public static final MessageFormat consumerPath=new MessageFormat("/meta/consumers/{0}/ids");
	
	private String topic;
	private ZkClient zkClient;
	
	public FindConsumer(String topic)
	{
		this.topic=topic;
		zkClient=new ZkClient(System.getenv("ZK_SERVER"),30000,30000,new ZkUtils.StringSerializer());
	}

	/**
	 * args[0]:topic
	 */
	public static void main(String[] args) 
	{
		//check(args);
		FindConsumer f=new FindConsumer(args[0]);
		f.find();
	}
	
	public void find()
	{
		List<String> groups=zkClient.getChildren(CONSUMER);
		for(String group:groups)
		{
			String ctp=consumerPartition.format(new Object[]{group,this.topic});
			if(zkClient.exists(ctp))
			{
				String cip=consumerPath.format(new Object[]{group});
				List<String> result=zkClient.getChildren(cip);
				System.out.println(group+":"+result);
			}
		}
	}
}

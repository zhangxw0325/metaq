package com.taobao.metamorphosis.tools.fresh;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;

import com.taobao.metamorphosis.utils.ZkUtils;

/**
 * args[0]:group
 * args[1]:topic
 * args[2]:method add,get
 * args[3]:partitions (optional seperate by comma)
 */
public class ChangeZKOffset extends Routine
{
	public static final String GET="get";
	public static final String SET="set";
	public static final long MSGID=1000000000000L;
	//0;group;1:topic;
	public static final MessageFormat partitionOffset=new MessageFormat("/meta/consumers/{0}/offsets/{1}");
	//0:brokerId
	public static final MessageFormat brokerPath=new MessageFormat("/meta/brokers/ids/{0}/master");
	//0:topic
	public static final MessageFormat topicPath=new MessageFormat("/meta/brokers/topics/{0}");
	
	private ZkClient zkClient;
	
	public ChangeZKOffset()
	{
		zkClient=new ZkClient(System.getenv("ZK_SERVER"),30000,30000,new ZkUtils.StringSerializer());
		//zkClient=new ZkClient("10.232.133.167",30000,30000,new ZkUtils.StringSerializer());
	}
	
	//path 在zk上存在更新原来的值，不存在的新增一个结点
	public void setZKOffset(long msgId,String offset,String path) throws Exception
	{
		if(offset!=null)
			ZkUtils.updatePersistentPath(zkClient, path, msgId+"-"+offset);
	}
	
	public void execute(String group,String topic,String method,String offset,Object... optional) throws Exception
	{
		String path=partitionOffset.format(new String[]{group,topic});
		
		if(GET.equals(method))
		{
			get(path);
		}
		else if(SET.equals(method))
		{
			System.out.println("get first");
			get(path);
			System.out.println("then set");
			set(topic,path,offset,optional);
			System.out.println("set success.");
		}
	}
	
	//获取zk中记录的消费端的offset
	public void get(String path)
	{
		if(zkClient.exists(path))
		{
			List<String> children=zkClient.getChildren(path);
			Collections.sort(children);
			for(String child:children)
			{
				String value=(String)zkClient.readData(path+"/"+child);
				System.out.println(child+" : "+value);
			}
		}
	}
	
	/*
	 * 设置zk中记录的消费端的offset值为指定值
	 * optional[0]:分区
	 * optional[1]:offset修改为该值;不设则修改为server offset
	 */
	public void set(String topic,String path,String offset,Object... optional) throws Exception
	{
		Map<String,String> bidIp=new HashMap<String,String>();
		Map<String,String> ipBid=new HashMap<String,String>();
		getBrokerIdIpMapping(bidIp,ipBid);
		
		String[] pids=null;
		String[] bids=null;
		List<String> bid_pids=new ArrayList<String>();;
		
		if(null==optional || null==optional[0])
		{
			getAll(bid_pids,topic);
		}
		else
		{
			pids=((String)optional[0]).split(",");
			for(String bid:bidIp.keySet())
			{
				for(String pid:pids)
				{
					bid_pids.add(bid+"-"+pid);
				}
			}
		}
		
		//key:topic_bid-pid value:offset
		Map<String,String> bidOffset=getBrokerOffset(ipBid,offset);
		
		Collections.sort(bid_pids);
		for(String bpid : bid_pids)
		{
			setZKOffset(MSGID,bidOffset.get(topic+"_"+bpid),path+"/"+bpid);
		}
	}
	
	//获取topic对应的分区列表
	public void getAll(List<String> bpids,String topic)
	{
		String path=topicPath.format(new String[]{topic});
		if(zkClient.exists(path))
		{
			List<String> children=zkClient.getChildren(path);
			for(String child:children)
			{
				//child: bid-m
				if(child.contains("-"))
				{
					int count=Integer.parseInt((String)zkClient.readData(path+"/"+child));
					for(int i=0;i<count;i++)
					{
						bpids.add(child.split("-")[0]+"-"+i);
					}
				}
			}
		}
	}
	
	public void getBrokerIdIpMapping(Map<String,String> bidIp,Map<String,String> ipBid)
	{
		String path="/meta/brokers/ids";
		if(zkClient.exists(path))
		{
			List<String> children=zkClient.getChildren(path);
			for(String brokerId:children)
			{
				String bPath=brokerPath.format(new String[]{brokerId});
				if(zkClient.exists(bPath))
				{
					String brokerIp=zkClient.readData(bPath);
					bidIp.put(brokerId, brokerIp);
					ipBid.put(brokerIp, brokerId);
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		check(args);
		new ChangeZKOffset().execute(args[0],args[1],args[2],args[3],args.length>4?args[4]:null);
		System.exit(0);
	}
}

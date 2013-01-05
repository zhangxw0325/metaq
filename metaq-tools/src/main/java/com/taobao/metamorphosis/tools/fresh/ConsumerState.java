package com.taobao.metamorphosis.tools.fresh;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.data.Stat;

import com.taobao.metamorphosis.utils.ZkUtils;

/**
 * 查询客户端消费情况(入参为group和topic)
 * 输出项：BID(brokerId),PID(partion),CID(consumerIp),BOFF(broker offset),COFF(consumer offset),DIF(BOFF和COFF的差值)
 */
public class ConsumerState extends Routine
{
	public static final String SEP="~~";
	//0:group
	public static final MessageFormat consumerPath=new MessageFormat("/meta/consumers/{0}/ids");
	//0:group;1:topic
	public static final MessageFormat consumerPartition=new MessageFormat("/meta/consumers/{0}/owners/{1}");
	//0;group;1:topic;2:patition
	public static final MessageFormat partitionOffset=new MessageFormat("/meta/consumers/{0}/offsets/{1}/{2}");
	//0:brokerId
	public static final MessageFormat brokerPath=new MessageFormat("/meta/brokers/ids/{0}/master");
	
	private ZkClient zkClient;
	
	public void stop()
	{
		this.zkClient.close();
	}
	
	public ConsumerState()
	{
		zkClient=new ZkClient(System.getenv("ZK_SERVER"),30000,30000,new ZkUtils.StringSerializer());
		//zkClient=new ZkClient("10.232.133.167",30000,30000,new ZkUtils.StringSerializer());
	}
	
	public List<String> getConsumer(String group)
	{
		List<String> consumers=new ArrayList<String>();
		String cPath=consumerPath.format(new String[]{group});
		if(zkClient.exists(cPath))
		{
			consumers=zkClient.getChildren(cPath);
		}
		else
		{
			System.out.println(cPath+" do not exsit!");
		}
		return consumers;
	}
	
	public List<String> getPartitions(String group,String topic)
	{
		List<String> ps=new ArrayList<String>();
		String partitionPath=consumerPartition.format(new String[]{group,topic});
		if(zkClient.exists(partitionPath))
		{
			List<String> partitionList=zkClient.getChildren(partitionPath);
			for(String p:partitionList)
			{
				String c=zkClient.readData(partitionPath+"/"+p);
				ps.add(p+SEP+c);
			}
		}
		else
		{
			System.out.println(partitionPath+" do not exsit!");
		}
		return ps;
	}
	
	public String getBrokerId(String partition)
	{
		String brokerId=null;
		Pattern pattern=Pattern.compile("(\\d*)-\\S*");
		Matcher matcher=pattern.matcher(partition);
		if(matcher.matches())
		{
			brokerId=matcher.group(1);
		}
		return brokerId;
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
	
	public List<Record> getOffset(List<String> partitions,String group,String topic)
	{
		List<Record> recordList=new ArrayList<Record>();
		Map<String,String> bidIp=new HashMap<String,String>();
		Map<String,String> ipBid=new HashMap<String,String>();
		getBrokerIdIpMapping(bidIp,ipBid);
		
		if(0!=partitions.size())
		{
			//get comsumer offset
			Map<String,ConsumerOffset> consumerOffsets=new HashMap<String,ConsumerOffset>();
			for(String p:partitions)
			{
				String cp=partitionOffset.format(new String[]{group,topic,p.split(SEP)[0]});
				if(zkClient.exists(cp))
				{
					Stat stat=new Stat();
					String offset=(String)zkClient.readData(cp,stat);
					consumerOffsets.put(p,new ConsumerOffset(offset,stat));
				}
			}
			Map<String,String> bidOffset=getBrokerOffset(ipBid,null);
			
			for(String key:consumerOffsets.keySet())
			{
				recordList.add(getRecord(bidIp,topic,key,consumerOffsets.get(key).offset,bidOffset,consumerOffsets.get(key).stat));
			}
		}
		else
		{
			System.out.println("patitions is 0!");
		}
		
		return recordList;
	}
	
	public Record getRecord(Map<String,String> bidIp,String topic,String bpi,String cOffset,Map<String,String> bidOffset,Stat stat)
	{
		Record record=new Record();
		Pattern pattern=Pattern.compile("(\\d*)-(\\d*)~~\\S*_(\\d+\\.\\d+\\.\\d+\\.\\d+)\\S*");
		Matcher matcher=pattern.matcher(bpi);
		if(matcher.matches())
		{
			record.bid=matcher.group(1)+"["+bidIp.get(matcher.group(1)).split("//")[1]+"]";
			record.pid=matcher.group(2);
			record.cid=matcher.group(3);
			String boff=bidOffset.get(topic+"_"+matcher.group(1)+"-"+matcher.group(2));
			if(null!=boff && !"".equals(boff))
				record.bOffset=Long.parseLong(boff);
			try
			{
			    String[] voffsets = cOffset.split("-");
			    
				record.cOffset=Long.parseLong(voffsets[voffsets.length-1]);
			}
			catch(Exception e)
			{
				//兼容老的
				record.cOffset=Long.parseLong(cOffset);
			}
			record.cTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(stat.getMtime()));
		}
		return record;
	}
	
	public void show(String group,String topic)
	{
		List<String> partitions=getPartitions(group, topic);
		List<Record> recordList=getOffset(partitions, group, topic);
		format(recordList);
		
	}
	
	public void format(List<Record> recordList)
	{
		System.out.printf("%s%s%s%s%s%s%s\n", flushLeft(25,"Broker ID"),flushLeft(14,"Partition ID"),
				flushLeft(18,"Consumer IP"),flushLeft(15,"Broker Offset"),flushLeft(16,"Consumer Offset"),
				flushLeft(14,"D-value"),flushLeft(22,"Modify Time"));
		for(Record record:recordList)
		{
			System.out.printf("%s%s%s%s%s%s%s\n", flushLeft(25,record.bid),flushLeft(14,record.pid),
					flushLeft(18,record.cid),flushLeft(15,record.bOffset+""),flushLeft(16,record.cOffset+""),
					flushLeft(14,(record.bOffset-record.cOffset)+""),flushLeft(22,record.cTime));
		}
	}
	
	//args[0]:group;args[1]:topic
	public static void main(String[] args)
	{
		check(args);
		new ConsumerState().show(args[0], args[1]);
		System.exit(0);
	}
	
	static class Record
	{
		String bid;
		String pid;
		String cid;
		long bOffset;
		long cOffset;
		String cTime;
		
		Record(){}
		Record(String bid,String pid,String cid,long bOff,long cOff,String cTime)
		{
			this.bid=bid;
			this.pid=pid;
			this.cid=cid;
			this.bOffset=bOff;
			this.cOffset=cOff;
			this.cTime=cTime;
		}
	}
	
	static class ConsumerOffset
	{
		public ConsumerOffset(){}
		public ConsumerOffset(String offset,Stat stat)
		{
			this.offset=offset;
			this.stat=stat;
		}
		String offset;
		Stat stat;
	}
}

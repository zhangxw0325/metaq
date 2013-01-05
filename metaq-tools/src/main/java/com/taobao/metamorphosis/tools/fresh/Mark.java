package com.taobao.metamorphosis.tools.fresh;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.data.Stat;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.StatsCommand;
import com.taobao.metamorphosis.utils.ZkUtils;

/**
 * 扩容标记位置工具,在所有的消费位置都超过mark的位置则开启扩容机器读功能
 * mark的位置记录在 $HOME/.meta_mark 文件中
 */
public class Mark extends Routine 
{
	//0:group 1:topic 2:consumer ip 3:brokerid-partition(ip) 4:coffset 5:checkpoint 6:live 7:done
	public static final MessageFormat outputFormat=new MessageFormat("{0}{1}{2}{3}{4}{5}{6}{7}{8}");
	//0:topic
	public static final MessageFormat topicPath=new MessageFormat("/meta/brokers/topics/{0}");
	//0:brokerId
	public static final MessageFormat brokerPath=new MessageFormat("/meta/brokers/ids/{0}/master");
	//0:group
	public static final MessageFormat consumerPath=new MessageFormat("/meta/consumers/{0}/ids");
	//0:group 1:topic
	public static final MessageFormat offsetPath=new MessageFormat("/meta/consumers/{0}/offsets/{1}");
	//0:group 1:topic 2:partition
	public static final MessageFormat offsetParPath=new MessageFormat("/meta/consumers/{0}/offsets/{1}/{2}");
	//0:group 1:topic 2:partition
	public static final MessageFormat consumerPartition=new MessageFormat("/meta/consumers/{0}/owners/{1}/{2}");
	public static final String CONSUMERPATH="/meta/consumers";
	
	private ZkClient zkClient;
	private String[] topics;
	private String[] bids;
	
	public Mark(String ts, String bs)
	{
		assert(null!=ts);
		assert(!"".equals(ts));
		topics=ts.split(",");
		if(null!=bs && !"".equals(bs))
			bids=bs.split(",");
		zkClient=new ZkClient(System.getenv("ZK_SERVER"),30000,30000,new ZkUtils.StringSerializer());
	}
	
	public void run()
	{
		//获取topics对应的meta服务器ip和bid对应关系
		Map<String, String> idIp=getBrokerIdIpMapping();
		
		//获取checkpoint.映射的key:topic_bid-partition,value:checkpoint
		Map<String, Long> checkPointMap=getCheckPoint(idIp);
		logCheckPoint(checkPointMap);
		
		int retry=3;
		while(true)
		{
			try
			{
				//循环检测group的消费状态是否达到checkpoint
				if(loopCheck(idIp, checkPointMap))
					break;
				Thread.sleep(10*1000);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				if(--retry<0)
				{
					System.err.println("error quit!");
					System.exit(-1);
				}
			}
		}
		
		System.out.println("all group reach the mark point.");
		stop();
		System.exit(0);
	}
	
	//0:group 1:topic 2:consumer ip 3:brokerid-partition(ip) 4:coffset 5:checkpoint 6:live 7:done
	public boolean loopCheck(Map<String, String> idIp, Map<String, Long> checkPointMap)
	{
		System.out.println();
		System.out.println(outputFormat.format(new String[]{flushLeft(28,"GROUP"),flushLeft(16,"TOPIC"),
				flushLeft(16,"CIP"),flushLeft(24,"BIP"),flushLeft(10,"DELAY"),flushLeft(12,"COFFSET"),
				flushLeft(12,"CHECKPOINT"),flushLeft(8,"LIVE"),flushLeft(8,"DONE")}));
				
		boolean finish=true;
		List<String> children=zkClient.getChildren(CONSUMERPATH);				
		//0
		for(String group:children)
		{
			//1
			for(String topic:topics)
			{
				String oPath=offsetPath.format(new String[]{group,topic});
				if(zkClient.exists(oPath))
				{
					List<String> partitions=zkClient.getChildren(oPath);
					for(String partition:partitions)
					{
						//2
						//zk上记录的消费端信息格式：zhu112_10.12.49.197-1350283552554-1
						String ipPath=consumerPartition.format(new String[]{group,topic,partition});
						String cip="";
						if(zkClient.exists(ipPath))
							cip=parseIp((String)zkClient.readData(ipPath),"\\S*_(\\d+\\.\\d+\\.\\d+\\.\\d+)\\S*");
						//3
						String bip=partition+"("+parseIp(idIp.get(partition.split("-")[0]),
								"\\S*//(\\d+\\.\\d+\\.\\d+\\.\\d+)\\S*")+")";
						//4
						String oPPath=offsetParPath.format(new String[]{group,topic,partition});
						Stat stat=new Stat();
						String value=zkClient.readData(oPPath, stat);
						long offset=illegalFormat(value) ? 0L : Long.parseLong(value.split("-")[1]);
						//5
						long checkPoint=checkPointMap.get(topic+"_"+partition);
						//6
						String cPath=consumerPath.format(new String[]{group});
						boolean live=zkClient.exists(cPath) && zkClient.getChildren(cPath).size()>0;
						//7
						boolean done=offset>=checkPoint;
						
						if(!done && !isDead(live,stat))
							finish=done;
						
						if(!isDead(live,stat))
							System.out.println(outputFormat.format(new String[]{flushLeft(28,group),
									flushLeft(16,topic),flushLeft(16,cip),flushLeft(24,bip),
									flushLeft(10,(System.currentTimeMillis()-stat.getMtime())+""),
									flushLeft(12,offset+""),flushLeft(12,checkPoint+""),
									flushLeft(8,live ? "true" : "false"),flushLeft(8,done ? "true" : "false")}));
					}
				}
			}
		}
		return finish;
	}
	
	public boolean isDead(boolean live, Stat stat)
	{
		//zk上面没有消费端信息live=false, 并且这种状态已经持续了超过六小时则认为消费端不再消费了
		if(!live && (System.currentTimeMillis()-stat.getMtime())>6*60*60*1000L)
			return true;
		return false;
	}
	
	public String parseIp(String value,String ps)
	{
		String ip="";
		if(null!=value && !"".equals(value))
		{
			Pattern pattern=Pattern.compile(ps);
			Matcher matcher=pattern.matcher(value);
			if(matcher.matches())
			{
				ip=matcher.group(1);
			}
		}
		return ip;
	}
	
	public boolean illegalFormat(String value)
	{
		if(null==value || "".equals(value) || -1==value.indexOf("-"))
			return true;
		return false;
	}
	
	public void stop()
	{
		zkClient.close();
	}
	
	public Map<String,String> getBrokerIdIpMapping()
	{
		Map<String,String> idIp=new HashMap<String,String>();
		for(String topic:topics)
		{
			String tPath=topicPath.format(new String[]{topic});
			if(zkClient.exists(tPath))
			{
				List<String> tChildern=zkClient.getChildren(tPath);
				for(String child:tChildern)
				{
					if(child.indexOf("-")>-1)
					{
						String bid=child.split("-")[0];
						String bPath=brokerPath.format(new String[]{bid});
						if(!exclude(bid) && zkClient.exists(bPath) && null==idIp.get(bid))
						{
							idIp.put(bid, (String)zkClient.readData(bPath));
						}
					}
				}
			}
		}
			
		return idIp;
	}
	
	public boolean exclude(String bid)
	{
		boolean exsit=false;
		if(null!=bids)
		{
			for(String b:bids)
			{
				if(b.equals(bid))
				{
					exsit=true;
					break;
				}
			}
		}
		return exsit;
	}
	
	public Map<String, Long> getCheckPoint(Map<String,String> idIp)
	{
		//key:topic_bid-partition,value:checkpoint
		Map<String, Long> checkPointMap=new HashMap<String, Long>();
		RemotingClientWrapper remotingClient=getRemotingClient();
		Set<Entry<String, String>> set=idIp.entrySet();
		for(Entry<String, String> entry:set)
		{
			try
			{
				remotingClient.connect(entry.getValue());
				remotingClient.awaitReadyInterrupt(entry.getValue());
				BooleanCommand resp =(BooleanCommand) remotingClient.invokeToGroup(entry.getValue(),
		        		new StatsCommand(OpaqueGenerator.getNextOpaque(), "offsets"), 
		        		3000, TimeUnit.MILLISECONDS);
				if(500==resp.getCode())
					System.out.println(resp.getErrorMsg());
				
				BufferedReader br=new BufferedReader(
						new InputStreamReader(new ByteArrayInputStream(resp.getErrorMsg().getBytes())));
				String line=null;
				while(null!=(line=br.readLine()))
				{
					String[] segs=line.split(" ");
					if(1<segs.length)
					{
						checkPointMap.put(segs[0]+"_"+entry.getKey()+"-"+segs[2], Long.parseLong(segs[6]));
					}
				}
				
				remotingClient.close(entry.getValue(), true);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		try
		{
			remotingClient.stop();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return checkPointMap;
	}
	
	public void logCheckPoint(Map<String, Long> checkPointMap)
	{
		try
		{
			String dir=System.getProperty("user.home");
			File file=new File(dir,".meta_mark");
			if(!file.exists())
				file.createNewFile();
			PrintWriter pw=new PrintWriter(file);
			for(Entry<String, Long> entry:checkPointMap.entrySet())
			{
				pw.println(entry.getKey()+"="+entry.getValue());
			}
			pw.flush();
			pw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	//args[0]为顺序消息topic列表,逗号分隔. 如: t1,t2,t3,t4
	//args[1]需要过滤的broker id列表,逗号分隔. 如: 101,102
	public static void main(String[] args) throws Exception
	{
		check(args);
		new Mark(args[0],args[1]).run();
	}
}

package com.taobao.metamorphosis.tools.fresh;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.network.AskCommand;
import com.taobao.metamorphosis.network.BooleanCommand;

public class ChangeZKOffsetByStamp extends ChangeZKOffset 
{
	public void set(String topic, String path, long timestamp) throws Exception 
	{
		Map<String,String> bidIp=new HashMap<String,String>();
		Map<String,String> ipBid=new HashMap<String,String>();
		getBrokerIdIpMapping(bidIp,ipBid);
		
		List<String> bid_pids=new ArrayList<String>();;
		
		getAll(bid_pids,topic);
		
		//key:topic_bid-pid value:offset
		Map<String,String> bidOffset=getBrokerOffset(ipBid,topic,timestamp);
		
		Collections.sort(bid_pids);
		for(String bpid : bid_pids)
		{
			setZKOffset(MSGID,bidOffset.get(topic+"_"+bpid),path+"/"+bpid);
		}
	}

	public void execute(String group,String topic,String method,String timestamp) throws Exception
	{
		long tt=Long.parseLong(timestamp);
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
			set(topic,path,tt);
			System.out.println("set success.");
		}
	}
	
	public Map<String, String> getBrokerOffset(Map<String, String> ipBid, String topic, long timestamp) 
	{
		//key:topic_brokerId_patitionId
		Map<String,String> bidOffset=new HashMap<String,String>();
		
		RemotingClientWrapper remotingClient=getRemotingClient();
		Set<String> ips=ipBid.keySet();
		for(String url:ips)
		{
			try
			{
				remotingClient.connect(url);
				remotingClient.awaitReadyInterrupt(url);
				BooleanCommand resp =(BooleanCommand) remotingClient.invokeToGroup(url,
		        		new AskCommand(topic, "offsets", new String[]{timestamp+""}), 3000, TimeUnit.MILLISECONDS);
				if(500==resp.getCode())
					System.out.println(resp.getErrorMsg());
		        parse(url,ipBid,bidOffset,resp.getErrorMsg());
		        remotingClient.close(url, true);
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
		
		return bidOffset;
	}

	//key:topic_bid-pid value:offset
	public void parse(String brokerIp, Map<String, String> ipBid, Map<String, String> bidOffset, String result)
	{
		BufferedReader br=new BufferedReader(new InputStreamReader(new ByteArrayInputStream(result.getBytes())));
		try
		{
			String line=null;
			while(null!=(line=br.readLine()))
			{
				String[] segs=line.split(" ");
				if(1<segs.length)
				{
					bidOffset.put(segs[0]+"_"+ipBid.get(brokerIp)+"-"+segs[2], segs[3]);
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		check(args);
		new ChangeZKOffsetByStamp().execute(args[0],args[1],args[2],args[3]);
		System.exit(0);
	}
}

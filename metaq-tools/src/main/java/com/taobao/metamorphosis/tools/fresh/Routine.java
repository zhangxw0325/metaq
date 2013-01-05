package com.taobao.metamorphosis.tools.fresh;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.RemotingFactory;
import com.taobao.gecko.service.config.ClientConfig;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.MetamorphosisWireFormatType;
import com.taobao.metamorphosis.network.StatsCommand;

public abstract class Routine 
{
	/**
	 * ZK_SERVER 系统环境变量,指定zk的地址
	 */
	public static void check(String[] args)
	{
		if(args.length<2)
		{
			help();
			System.exit(1);
		}
		String zkServer=System.getenv("ZK_SERVER");
		if(null==zkServer || !addressCheck(zkServer))
		{
			System.out.println("set ZK_SERVER environment parameter first");
			System.exit(2);
		}
	}
	
	public static boolean addressCheck(String addrs)
	{
		Pattern pattern=Pattern.compile("^\\d+[\\d,:.]*");
		Matcher matcher=pattern.matcher(addrs.trim());
		return matcher.matches();
	}
	
	public static void help()
	{
		System.out.println("Usage:....");
	}
	
	public static String flushLeft(long length, String content)
	{
		return flushLeft(' ',length,content);
	}
	public static String flushLeft(char c, long length, String content)
	{
		String str = "",cs = "";   
	    if (content.length() > length)
	    {   
	    	str = content;   
	    }
	    else
	    {  
	    	for (int i = 0; i < length - content.length(); i++)
	    	{   
	    		cs = cs + c;   
	    	}
	    }
	    str = content + cs;    
	    return str;  
	}
	
	public Map<String,String> getBrokerOffset(Map<String,String> ipBid,String offset)
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
		        		new StatsCommand(OpaqueGenerator.getNextOpaque(), "offsets"), 3000, TimeUnit.MILLISECONDS);
				if(500==resp.getCode())
					System.out.println(resp.getErrorMsg());
		        parse(url,ipBid,bidOffset,resp.getErrorMsg(),offset);
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
	
	public RemotingClientWrapper getRemotingClient()
	{
		RemotingClientWrapper remotingClient=null;
		final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setTcpNoDelay(false);
        clientConfig.setWireFormatType(new MetamorphosisWireFormatType());
        clientConfig.setMaxScheduleWrittenBytes(Runtime.getRuntime().maxMemory() / 3);
        try
        {
        	remotingClient=new RemotingClientWrapper(RemotingFactory.connect(clientConfig));
        }
        catch(Exception e)
        {
        	System.out.println(e);
        	System.exit(4);
        }
        return remotingClient;
	}
	
	//key:topic_bid-pid value:offset
	public void parse(String brokerIp,Map<String,String> ipBid,Map<String,String> bidOffset,String result,String off)
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
					bidOffset.put(segs[0]+"_"+ipBid.get(brokerIp)+"-"+segs[2], null==off?segs[6]:off);
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}

package com.taobao.metamorphosis.tools.fresh;

import java.text.MessageFormat;

import org.I0Itec.zkclient.ZkClient;

import com.taobao.metamorphosis.utils.ZkUtils;

/**
 * 设置consumer端是从master上还是slave上拉取消息
 * args[0]:group
 * args[1]:slaveId(-1表示从master上拉取消息, null表示server不做控制由客户端自行控制)
 */
public class ConsumerRetrieveFrom extends Routine
{
	// 0:group
	public static final MessageFormat CONSUMER_STANDBY=new MessageFormat("/meta/consumers/{0}/standby");
	private ZkClient zkClient;
	private String group;
	private String slaveId;
	
	public ConsumerRetrieveFrom(String group,String slaveId)
	{
		this.group=group;
		this.slaveId=slaveId;
		zkClient=new ZkClient(System.getenv("ZK_SERVER"),30000,30000,new ZkUtils.StringSerializer());
	}
	
	public static void main(String[] args) 
	{
		check(args);
		ConsumerRetrieveFrom crf=new ConsumerRetrieveFrom(args[0],args[1]);
		crf.execute();
	}
	
	public void execute()
	{
		System.out.println("before change standby node ,the value is "+getSlaveIdFromZK());
		setSlaveId();
		System.out.println("after change standby node ,the value is "+getSlaveIdFromZK());
	}
	
	public String getSlaveIdFromZK()
	{
		String csPath=CONSUMER_STANDBY.format(new Object[]{group});
		if(zkClient.exists(csPath))
		{
			return (String)zkClient.readData(csPath);
		}
		
		return "null";
	}
	
	public void setSlaveId()
	{
		String csPath=CONSUMER_STANDBY.format(new Object[]{group});
		if(!zkClient.exists(csPath))
			zkClient.createPersistent(csPath, this.slaveId);
		else
			zkClient.writeData(csPath, this.slaveId);
	}
}

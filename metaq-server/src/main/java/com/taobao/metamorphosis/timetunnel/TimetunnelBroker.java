package com.taobao.metamorphosis.timetunnel;

import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.AbstractBrokerPlugin;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.MessageConsumer;
import com.taobao.timetunnel.client.SessionFactory;
import com.taobao.timetunnel.client.TimeTunnelClientException;
import com.taobao.timetunnel.client.TimeTunnelSessionFactory;
import com.taobao.timetunnel.client.conf.ConsumerConfig;
import com.taobao.timetunnel.client.conf.TimeTunnelConfig;


/**
 * @author 无花
 * @since 1.0.0 ,2011-6-7 上午11:56:34
 */

public class TimetunnelBroker extends AbstractBrokerPlugin {

    
    private MessageConsumer consumer;
    private CommandProcessor commandProcessor;

    @Override
    public void start() {
        try {
        	this.consumerMessage();
		} catch (TimeTunnelClientException e) {
			log.error("consumerMessage error", e);
			throw new MetamorphosisServerStartupException(e.toString());
		}
    }


    private void consumerMessage() throws TimeTunnelClientException {
    	
    	while(true) {
            Iterator<Message> msgsIte = consumer.iterator();
            while(msgsIte.hasNext()) {
                Message msg = msgsIte.next();
                processMessage(msg);
            }
        }
    			
	}


	private void processMessage(Message msg) {
		if (msg == null) {
            log.warn("no message received form timetunnel");
            return;
        }
        try {
            final byte[] data = msg.getData();
            final PutCommand request = new PutCommand(msg.getTag(), -1, data, null, 0, 0);
            this.commandProcessor.processPutCommand(request, null, null);
        }
        catch (final Throwable e) {
            log.error("处理timetunnel消息失败.Append timetunnel message failed,topic=" + msg.getTag(), e);
        }
	}


	@Override
    public void stop() {
//        this.client.close();
    }


    @Override
    public void init(final MetaMorphosisBroker metaMorphosisBroker, final Properties props) {
        this.broker = metaMorphosisBroker;
        this.props = props;
        this.commandProcessor = metaMorphosisBroker.getBrokerProcessor();
        final String ttServerList = this.props.getProperty("ttServerList");
        final String user = this.props.getProperty("user");
        final String password = this.props.getProperty("password");
        final String queueForApi = this.props.getProperty("queueForApi");
        final String subscriberId = this.props.getProperty("subscriberId");
        
        if (StringUtils.isEmpty(ttServerList)||StringUtils.isEmpty(user)||StringUtils.isEmpty(password)) {
            throw new MetamorphosisServerStartupException("Timetunnel ServerList or user or password is empty");
        }
        
        // 设置配置
        final TimeTunnelConfig config = new TimeTunnelConfig(queueForApi); //设置Queue名字
        config.setRouterURL(ttServerList); //设置TT的路由集群
        config.setUser(user); //设置访问TT集群的用户
        config.setPassword(password); //设置访问TT集群的用户密码

        SessionFactory sessionFactory = TimeTunnelSessionFactory.getInstance();
        final ConsumerConfig consumerConfig = new ConsumerConfig(config);
        consumerConfig.setSubscriberId(subscriberId); //设置订阅ID，ID需要向TT管理员申请（许远，火龙）
        String tagFilters = parseTagsFilter((String) this.props.get("ttTopicTags"));
        consumerConfig.setFetchFilter(tagFilters);
        try {
			consumer = sessionFactory.createConsumer(consumerConfig);
		} catch (TimeTunnelClientException e) {
			log.error("create Timetunnel consumer error!", e);
			throw new MetamorphosisServerStartupException("create Timetunnel consumer error!");
		}
    }


    private String parseTagsFilter(String ttTopicTags) {
    	String tags = "";
    	final String[] topicTagStr = StringUtils.split(ttTopicTags, ";");
        if (topicTagStr == null || topicTagStr.length == 0) {
        	throw new RuntimeException("timetunnel topic is empty");
        }
        for(String tag:topicTagStr){
        	if(StringUtils.isBlank(tags)){
        		tags +="__subtopic__="+tag;
        	}else{
        		tags +=" or __subtopic__="+tag;
        	}
        		
        }
        return tags;
	}


	@Override
    public String name() {
        return "timetunnel";
    }


}

package com.taobao.metamorphosis.metaslave;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.AbstractBrokerPlugin;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;


public class MetamorphosisSlaveBroker extends AbstractBrokerPlugin {

	public static final String NAME = "metaslave";
	
    private PullMessageController pullMessageController;


    @Override
    public void init(final MetaMorphosisBroker metaMorphosisBroker, final Properties props) {
        this.broker = metaMorphosisBroker;
        this.props = props;

        this.putSlaveProperties(this.broker, this.props);

        if (!this.broker.getMetaConfig().isSlave()) {
            throw new SubscribeMasterMessageException("Could not start as a slave broker");
        }

        try {
            this.pullMessageController = new PullMessageController(this.broker);
        }
        catch (final MetaClientException e) {
            throw new SubscribeMasterMessageException("Create subscribeHandler failed", e);
        }
    }


    private void putSlaveProperties(MetaMorphosisBroker broker, Properties props) {
        broker.getMetaConfig().setSlaveId(Integer.parseInt(props.getProperty("slaveId")));
        if (StringUtils.isNotBlank(props.getProperty("slaveGroup"))) {
            broker.getMetaConfig().setSlaveGroup(props.getProperty("slaveGroup"));
        }
        if (StringUtils.isNotBlank(props.getProperty("slaveMaxDelayInMills"))) {
            broker.getMetaConfig().setSlaveMaxDelayInMills(
                Integer.parseInt(props.getProperty("slaveMaxDelayInMills")));
        }

        // 重新设置BrokerIdPath，以便注册到slave的路径
        broker.getBrokerZooKeeper().resetBrokerIdPath();
    }


    @Override
    public String name() {
        return NAME;
    }


    @Override
    public void start() {
        try {
            this.pullMessageController.start();
        }
        catch (NotifyRemotingException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void stop() {
        try {
            this.pullMessageController.shutdown();
        }
        catch (NotifyRemotingException e) {
            e.printStackTrace();
        }
    }
}

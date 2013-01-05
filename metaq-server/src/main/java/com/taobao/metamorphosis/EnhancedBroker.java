package com.taobao.metamorphosis;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;


/**
 * @author 无花
 * @since 2011-6-9 下午01:27:12
 */

public class EnhancedBroker {

    static final Log log = LogFactory.getLog(EnhancedBroker.class);

    private final MetaMorphosisBroker broker;

    private BrokerPlugins brokerPlugins;


    public void start() throws Exception {
        // 先启动meta,然后启动Plugins
        this.broker.start();
        this.brokerPlugins.start();
    }


    public void stop() {
        this.brokerPlugins.stop();
        this.broker.stop();
    }


    public EnhancedBroker(MetaConfig metaConfig, Map<String/* plugin name */, Properties> pluginsInfo) {
        this.broker = new MetaMorphosisBroker(metaConfig);
        this.brokerPlugins = new BrokerPlugins(pluginsInfo, broker);
        this.brokerPlugins.init(broker, null);
    }


    public MetaMorphosisBroker getBroker() {
        return broker;
    }

}

package com.taobao.metamorphosis;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.assembly.BrokerCommandProcessor;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;


/**
 * @author ÎÞ»¨
 * @since 2011-6-9 ÏÂÎç07:57:46
 */

public class ServerStartupTest {
    private MetaMorphosisBroker metaMorphosisBroker;
    private BrokerCommandProcessor brokerCommandProcessor;
    private BrokerZooKeeper brokerZooKeeper;
    private IMocksControl mocksControl;


    @Before
    public void setup() {
        this.mocksControl = EasyMock.createControl();
        this.metaMorphosisBroker = this.mocksControl.createMock(MetaMorphosisBroker.class);
        this.brokerCommandProcessor = this.mocksControl.createMock(BrokerCommandProcessor.class);
        this.brokerZooKeeper = this.mocksControl.createMock(BrokerZooKeeper.class);

    }


    @Test
    public void testGetPluginsInfo() {
        final String[] args =
                StringUtils.split(
                    "./meta-server-start.sh -f ../conf/server.properties -Fnotify notifySlave.properties", " ");
        final CommandLine line = StartupHelp.parseCmdLine(args, new PosixParser());
        final Map<String, Properties> pluginsInfo = ServerStartup.getPluginsInfo(line);
        Assert.assertTrue(pluginsInfo.get("notify").getProperty("notify-groupId").equals("\"meta-slave1\""));

        // EasyMock.expect(this.metaMorphosisBroker.).andReturn(this.brokerCommandProcessor)
        // .anyTimes();
        // EasyMock.expect(this.metaMorphosisBroker.getMetaConfig()).andReturn(new
        // MetaConfig());
        EasyMock.expect(this.metaMorphosisBroker.getBrokerProcessor()).andReturn(null);
        this.mocksControl.replay();

        final BrokerPlugins brokerPlugins = new BrokerPlugins(pluginsInfo, this.metaMorphosisBroker);
        brokerPlugins.init(this.metaMorphosisBroker, null);
        this.mocksControl.verify();
        Assert.assertTrue(brokerPlugins.getPluginsInfo().size() == 1);
        Assert.assertTrue(brokerPlugins.getPluginsInfo().containsKey("notify"));
    }


    @Test
    public void testGetPluginsInfo_metaslave() {
        final String[] args =
                StringUtils.split(
                    "./meta-server-start.sh -f ../conf/server.properties -Fmetaslave async_slave.properties", " ");
        final CommandLine line = StartupHelp.parseCmdLine(args, new PosixParser());
        final Map<String, Properties> pluginsInfo = ServerStartup.getPluginsInfo(line);
        Assert.assertTrue(pluginsInfo.get("metaslave").getProperty("slaveId").equals("1"));
        Assert.assertTrue(pluginsInfo.get("metaslave").getProperty("slaveGroup").equals("meta-slave-group"));
        Assert.assertTrue(pluginsInfo.get("metaslave").getProperty("slaveMaxDelayInMills").equals("500"));

        EasyMock.expect(this.metaMorphosisBroker.getBrokerZooKeeper()).andReturn(brokerZooKeeper).anyTimes();
        // this.brokerZooKeeper.resetBrokerIdPath();
        // EasyMock.expect(this.metaMorphosisBroker.getStoreManager()).andReturn(null);
        // EasyMock.expect(metaMorphosisBroker.getIdWorker()).andReturn(null);
        // EasyMock.expect(this.metaMorphosisBroker.getStatsManager()).andReturn(null);
        // EasyMock.expect(this.metaMorphosisBroker.getBrokerProcessor()).andReturn(this.brokerCommandProcessor);
        final MetaConfig metaConfig = new MetaConfig();
        // metaConfig.setSlaveId(1);
        EasyMock.expect(this.metaMorphosisBroker.getMetaConfig()).andReturn(metaConfig).anyTimes();
        this.mocksControl.replay();

        final BrokerPlugins brokerPlugins = new BrokerPlugins(pluginsInfo, this.metaMorphosisBroker);
        // brokerPlugins.init(this.metaMorphosisBroker, null);
        this.mocksControl.verify();

        Assert.assertTrue(brokerPlugins.getPluginsInfo().size() == 1);
        Assert.assertTrue(brokerPlugins.getPluginsInfo().containsKey("metaslave"));
    }
}

package com.taobao.metamorphosis.server.network;

import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;

import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.assembly.BrokerCommandProcessor;
import com.taobao.metamorphosis.server.assembly.ExecutorsManager;
import com.taobao.metamorphosis.server.assembly.MessageTypeManager;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.IdWorker;
import com.taobao.metaq.store.MetaStore;


public abstract class BaseProcessorUnitTest {

    protected MessageStoreManager storeManager;
    protected MetaConfig metaConfig;
    protected Connection conn;
    protected IMocksControl mocksControl;
    protected BrokerCommandProcessor commandProcessor;
    protected StatsManager statsManager;
    protected IdWorker idWorker;
    protected BrokerZooKeeper brokerZooKeeper;
    protected ExecutorsManager executorsManager;
    protected SessionContext sessionContext;
    protected MessageTypeManager messageTypeManager;
    protected MetaStore metaStore;


    protected void mock() {

        this.metaConfig = new MetaConfig();
        this.mocksControl = EasyMock.createControl();
        this.storeManager = this.mocksControl.createMock(MessageStoreManager.class);
        this.conn = this.mocksControl.createMock(Connection.class);
        this.sessionContext = new SessionContextImpl(null, this.conn);
        EasyMock.expect(this.conn.getAttribute(SessionContextHolder.GLOBAL_SESSION_KEY)).andReturn(this.sessionContext)
            .anyTimes();
        this.statsManager = new StatsManager(new MetaConfig(), null, null, null);
        this.idWorker = this.mocksControl.createMock(IdWorker.class);
        this.brokerZooKeeper = this.mocksControl.createMock(BrokerZooKeeper.class);
        this.executorsManager = this.mocksControl.createMock(ExecutorsManager.class);
        this.messageTypeManager = this.mocksControl.createMock(MessageTypeManager.class);
        this.metaStore = this.mocksControl.createMock(MetaStore.class);
        this.commandProcessor = new BrokerCommandProcessor();
        this.commandProcessor.setMetaConfig(this.metaConfig);
        this.commandProcessor.setStoreManager(this.storeManager);
        this.commandProcessor.setStatsManager(this.statsManager);
        this.commandProcessor.setBrokerZooKeeper(this.brokerZooKeeper);
        this.commandProcessor.setIdWorker(this.idWorker);
        this.commandProcessor.setExecutorsManager(this.executorsManager);
        this.commandProcessor.setMessageTypeManager(this.messageTypeManager);
        this.commandProcessor.setMetaStore(this.metaStore);
    }

}

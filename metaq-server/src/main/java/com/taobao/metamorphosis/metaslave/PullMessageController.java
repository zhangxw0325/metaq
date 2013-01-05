/**
 * $Id: PullMessageController.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.metaslave;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.core.util.OpaqueGenerator;
import com.taobao.gecko.service.RemotingFactory;
import com.taobao.gecko.service.config.ClientConfig;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.client.RemotingClientWrapper;
import com.taobao.metamorphosis.exception.NetworkException;
import com.taobao.metamorphosis.network.DataCommand;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.MetamorphosisWireFormatType;
import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metaq.commons.MetaMessageDecoder;
import com.taobao.metaq.commons.ServiceThread;
import com.taobao.metaq.store.MetaStore;


public class PullMessageController {
    public final static String SlaveLogName = "MetaSlave";
    private final static Logger log = Logger.getLogger(SlaveLogName);

    private final MetaMorphosisBroker broker;
    private final SlaveZooKeeper slaveZooKeeper;
    private final RemotingClientWrapper remotingClient;
    private final SlaveOffsetStorage slaveOffsetStorage;
    private String masterServerUrl = null;
    private volatile boolean masterFixed = false;
    private volatile boolean started = false;

    private final PullService pullService;


    public PullMessageController(MetaMorphosisBroker broker) throws NetworkException {
        this.broker = broker;
        this.slaveZooKeeper = new SlaveZooKeeper(this.broker, this);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setTcpNoDelay(false);
        clientConfig.setWireFormatType(new MetamorphosisWireFormatType());
        clientConfig.setMaxScheduleWrittenBytes(Runtime.getRuntime().maxMemory() / 3);
        try {
            this.remotingClient = new RemotingClientWrapper(RemotingFactory.connect(clientConfig));
        }
        catch (final NotifyRemotingException e) {
            throw new NetworkException("Create remoting client failed", e);
        }

        this.slaveOffsetStorage = new SlaveOffsetStorage(this.broker, this.slaveZooKeeper, this.remotingClient);
        this.pullService = new PullService();
    }

    class PullService extends ServiceThread {
        private final static int PullMaxSize = 1024 * 1024;
        private final static int PrintInterval = 1000 * 10;
        private volatile long pullFromOffset = 0;
        private volatile long lastPrintTimestamp = System.currentTimeMillis();


        private byte[] invokeToMaster() {
            MetaConfig metaConfig = PullMessageController.this.broker.getMetaConfig();

            final GetCommand getCmd =
                    new GetCommand("slave", metaConfig.getSlaveGroup(), 0, pullFromOffset, PullMaxSize,
                        OpaqueGenerator.getNextOpaque());
            final String serverUrl = PullMessageController.this.masterServerUrl;
            try {
                ResponseCommand response =
                        PullMessageController.this.remotingClient.invokeToGroup(serverUrl, getCmd, 10 * 1000,
                            TimeUnit.MILLISECONDS);

                if (response instanceof DataCommand) {
                    final DataCommand dataCmd = (DataCommand) response;
                    final byte[] data = dataCmd.getData();
                    return data;
                }
                else {
                    if ((System.currentTimeMillis() - this.lastPrintTimestamp) > PrintInterval) {
                        this.lastPrintTimestamp = System.currentTimeMillis();
                        PullMessageController.log.info("slave pull from master, but no data "
                                + this.pullFromOffset);
                    }
                }
            }
            catch (InterruptedException e) {
                PullMessageController.log.error("PullService.invokeToMaster exception", e);
            }
            catch (TimeoutException e) {
                PullMessageController.log.error("PullService.invokeToMaster exception", e);
            }
            catch (NotifyRemotingException e) {
                PullMessageController.log.error("PullService.invokeToMaster exception", e);
            }

            return null;
        }


        private void doPull() {
            for (byte[] data = this.invokeToMaster(); data != null && !this.isStoped(); data =
                    this.invokeToMaster()) {
                long startAppendOffset = this.pullFromOffset;
                // 第一次拉
                if (0 == this.pullFromOffset) {
                    java.nio.ByteBuffer byteBuffer = java.nio.ByteBuffer.wrap(data);
                    byteBuffer.flip();
                    byteBuffer.limit(data.length);
                    startAppendOffset = byteBuffer.getLong(MetaMessageDecoder.MessagePhysicOffsetPostion);
                    PullMessageController.log.info("slave pull from offset fixed " + startAppendOffset);
                }

                MetaStore metaStore = PullMessageController.this.broker.getMetaStoreMaster();
                boolean appendResult = metaStore.appendToPhyQueue(startAppendOffset, data);
                if (appendResult) {
                    this.pullFromOffset = startAppendOffset + data.length;
                }
                else {
                    PullMessageController.log.fatal("slave append data error");
                    break;
                }
            }
        }


        @Override
        public void run() {
            PullMessageController.log.info(this.getServiceName() + " service started");
            while (!PullMessageController.this.isMasterFixed() && !this.isStoped()) {
                PullMessageController.log.info("waiting for master starting");
                try {
                    Thread.sleep(3000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (!this.isStoped()) {
                PullMessageController.this.connectMaster();
            }

            long maxPullInterval = PullMessageController.this.broker.getMetaConfig().getSlaveMaxDelayInMills();

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(maxPullInterval);
                    this.doPull();
                }
                catch (Exception e) {
                    PullMessageController.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            PullMessageController.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return PullService.class.getName();
        }


        public long getPullFromOffset() {
            return pullFromOffset;
        }


        public void setPullFromOffset(long pullFromOffset) {
            this.pullFromOffset = pullFromOffset;
        }
    }


    public void start() throws NotifyRemotingException {
        if (!this.started) {
            this.remotingClient.start();
            this.slaveZooKeeper.start();
            try {
                Thread.sleep(3000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.masterServerUrl = this.slaveZooKeeper.getMasterServerUrl();
            this.masterFixed = (this.masterServerUrl != null);

            this.pullService.setPullFromOffset(this.broker.getMetaStoreMaster().getMaxPhyOffset());
            this.pullService.start();
            this.started = true;
        }
    }


    public void shutdown() throws NotifyRemotingException {
        if (this.started) {
            this.remotingClient.stop();
            this.pullService.shutdown();
            this.started = false;
        }
    }


    public void connectMaster() {
        final int RetryTimes = 3;
        if (this.isMasterFixed()) {
            for (int i = 0; i < RetryTimes; i++) {
                try {
                    this.remotingClient.connect(this.masterServerUrl);
                    i = RetryTimes;
                    log.info("############## slave connect master OK, " + this.masterServerUrl);
                }
                catch (NotifyRemotingException e) {
                    log.warn("connectMaster error", e);
                }
            }
        }
    }


    public String getMasterServerUrl() {
        return masterServerUrl;
    }


    public void setMasterServerUrl(String masterServerUrl) {
        this.masterServerUrl = masterServerUrl;
    }


    public boolean isMasterFixed() {
        return masterFixed;
    }


    public void setMasterFixed(boolean masterFixed) {
        this.masterFixed = masterFixed;
    }


    public boolean isStarted() {
        return started;
    }


    public void setStarted(boolean started) {
        this.started = started;
    }


    public static String getSlavelogname() {
        return SlaveLogName;
    }


    public static Logger getLog() {
        return log;
    }


    public MetaMorphosisBroker getBroker() {
        return broker;
    }


    public SlaveZooKeeper getSlaveZooKeeper() {
        return slaveZooKeeper;
    }


    public RemotingClientWrapper getRemotingClient() {
        return remotingClient;
    }


    public SlaveOffsetStorage getSlaveOffsetStorage() {
        return slaveOffsetStorage;
    }
}

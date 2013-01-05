/**
 * $Id: DefaultMetaStore.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.taobao.metaq.commons.MetaMessage;
import com.taobao.metaq.commons.MetaMessageAnnotation;
import com.taobao.metaq.commons.MetaMessageDecoder;
import com.taobao.metaq.commons.MetaMessageWrapper;
import com.taobao.metaq.commons.MetaUtil;
import com.taobao.metaq.commons.ServiceThread;
import com.taobao.metaq.commons.SystemClock;
import com.taobao.metaq.store.DefaultMetaStore.DispatchMessageService.DispatchRequest;


/**
 * 存储层默认实现
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class DefaultMetaStore implements MetaStore {
    private static final Logger log = Logger.getLogger(MetaStore.MetaStoreLogName);
    // 存储服务是否启动
    private volatile boolean shutdown = true;
    // 消息过滤
    private final MessageFilter messageFilter = new DefaultMessageFilter();
    // 存储配置
    private final MetaStoreConfig metaStoreConfig;
    // 物理队列
    private final MetaQueuePhysical metaQueuePhysical;
    // 逻辑队列集合
    private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, MetaQueueLogistic>> metaQueueLogisticTable;
    // 逻辑队列刷盘服务
    private final FlushLogicQueueService flushLogicQueueService;
    // 清理物理文件服务
    private final CleanPhysicFileService cleanPhysicFileService;
    // 清理逻辑文件服务
    private final CleanLogicsFileService cleanLogicsFileService;
    // 分发消息索引服务
    private final DispatchMessageService dispatchMessageService;
    // 预分配MapedFile对象服务
    private final AllocateMapedFileService allocateMapedFileService;
    // 从物理队列解析消息重新发送到逻辑队列
    private final ReputMessageService reputMessageService;
    // 运行时数据统计
    private final MetaStatsService metaStatsService;
    // 运行过程标志位
    private final RunningFlags runningFlags = new RunningFlags();
    // 存储检查点
    private StoreCheckpoint storeCheckpoint;
    // 权限控制后，打印间隔次数
    private AtomicLong printTimes = new AtomicLong(0);
    // 优化获取时间性能，精度1ms
    private final SystemClock systemClock = new SystemClock(1);


    public DefaultMetaStore(final MetaStoreConfig metaStoreConfig) {
        this.metaStoreConfig = metaStoreConfig;
        this.allocateMapedFileService = new AllocateMapedFileService();
        this.metaQueuePhysical = new MetaQueuePhysical(this);
        this.metaQueueLogisticTable =
                new ConcurrentHashMap<String/* topic */, ConcurrentHashMap<Integer/* queueId */, MetaQueueLogistic>>(
                    32);

        this.flushLogicQueueService = new FlushLogicQueueService();
        this.cleanPhysicFileService = new CleanPhysicFileService();
        this.cleanLogicsFileService = new CleanLogicsFileService();
        this.dispatchMessageService = new DispatchMessageService(this.metaStoreConfig.getPutMsgIndexHightWater());
        this.metaStatsService = new MetaStatsService();

        if (!this.metaStoreConfig.isMaster()) {
            this.reputMessageService = new ReputMessageService();
        }
        else {
            this.reputMessageService = null;
        }

        // load过程依赖此服务，所以提前启动
        this.allocateMapedFileService.start();
    }

    /**
     * 清理物理文件服务
     */
    class CleanPhysicFileService extends ServiceThread {
        // 磁盘空间警戒水位，超过，则停止接收新消息（出于保护自身目的）
        private final double DiskSpaveWarningLevelRatio = 0.90;

        private long lastRedeleteTimestamp = 0;

        // 手工触发一次最多删除次数
        private final static int MaxManualDeleteFileTimes = 20;

        // 手工触发删除消息
        private volatile int manualDeleteFileSeveralTimes = 0;


        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MaxManualDeleteFileTimes;
            DefaultMetaStore.log.info("excuteDeleteFilesManualy was invoked");
        }


        /**
         * 是否可以删除文件，时间是否满足
         */
        private boolean isTimeToDelete() {
            String when = DefaultMetaStore.this.getMetaStoreConfig().getDeleteWhen();
            if (MetaUtil.isItTimeToDo(when)) {
                DefaultMetaStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }


        /**
         * 是否可以删除文件，空间是否满足
         */
        private boolean isSpaceToDelete() {
            double ratio = DefaultMetaStore.this.getMetaStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            // 检测物理文件磁盘空间
            {
                String storePathPhysic = DefaultMetaStore.this.getMetaStoreConfig().getStorePathPhysic();
                double physicRatio = MetaUtil.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                if (physicRatio > DiskSpaveWarningLevelRatio) {
                    boolean diskok = DefaultMetaStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMetaStore.log.fatal("physic disk maybe full soon " + physicRatio
                                + ", so mark disk full");
                        System.gc();
                    }
                }
                else {
                    boolean diskok = DefaultMetaStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMetaStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMetaStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            // 检测逻辑文件磁盘空间
            {
                String storePathLogics = DefaultMetaStore.this.getMetaStoreConfig().getStorePathLogics();
                double logicsRatio = MetaUtil.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > DiskSpaveWarningLevelRatio) {
                    boolean diskok = DefaultMetaStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMetaStore.log.fatal("logics disk maybe full soon " + logicsRatio
                                + ", so mark disk full");
                        System.gc();
                    }
                }
                else {
                    boolean diskok = DefaultMetaStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMetaStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMetaStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }
            return false;
        }


        private void deleteExpiredFiles() {
            int deleteCount = 0;
            long fileReservedTime = DefaultMetaStore.this.getMetaStoreConfig().getFileReservedTime();
            int deletePhysicFilesInterval =
                    DefaultMetaStore.this.getMetaStoreConfig().getDeletePhysicFilesInterval();
            int destroyMapedFileIntervalForcibly =
                    DefaultMetaStore.this.getMetaStoreConfig().getDestroyMapedFileIntervalForcibly();

            boolean timeup = this.isTimeToDelete();
            boolean spacefull = this.isSpaceToDelete();
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            // 删除物理队列文件
            if (timeup || spacefull || manualDelete) {
                log.info("begin to delete before " + fileReservedTime + " hours file. timeup: " + timeup
                        + " spacefull: " + spacefull + " manualDeleteFileSeveralTimes: "
                        + this.manualDeleteFileSeveralTimes);

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                // 小时转化成毫秒
                fileReservedTime *= 60 * 60 * 1000;
                deleteCount =
                        DefaultMetaStore.this.metaQueuePhysical.deleteExpiredFile(fileReservedTime,
                            deletePhysicFilesInterval, destroyMapedFileIntervalForcibly);
                if (deleteCount > 0) {
                    DefaultMetaStore.this.cleanLogicsFileService.wakeup();
                }
                // 危险情况：磁盘满了，但是又无法删除文件
                else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }


        /**
         * 最前面的文件有可能Hang住，定期检查一下
         */
        private void redeleteHangedFile() {
            int interval = DefaultMetaStore.this.getMetaStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMapedFileIntervalForcibly =
                        DefaultMetaStore.this.getMetaStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMetaStore.this.metaQueuePhysical.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                    DefaultMetaStore.this.cleanLogicsFileService.wakeup();
                }
            }
        }


        public void run() {
            DefaultMetaStore.log.info(this.getServiceName() + " service started");
            int cleanResourceInterval = DefaultMetaStore.this.getMetaStoreConfig().getCleanResourceInterval();
            while (!this.isStoped()) {
                try {
                    this.waitForRunning(cleanResourceInterval);

                    this.deleteExpiredFiles();

                    this.redeleteHangedFile();
                }
                catch (Exception e) {
                    DefaultMetaStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMetaStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return CleanPhysicFileService.class.getSimpleName();
        }


        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }


        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
    }

    /**
     * 清理逻辑文件服务
     */
    class CleanLogicsFileService extends ServiceThread {
        private long lastPhysicalMinOffset = 0;


        private void deleteExpiredFiles() {
            int deleteLogicsFilesInterval =
                    DefaultMetaStore.this.getMetaStoreConfig().getDeleteLogicsFilesInterval();

            long minOffset = DefaultMetaStore.this.metaQueuePhysical.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                // 删除逻辑队列文件
                ConcurrentHashMap<String, ConcurrentHashMap<Integer, MetaQueueLogistic>> tables =
                        DefaultMetaStore.this.metaQueueLogisticTable;

                for (ConcurrentHashMap<Integer, MetaQueueLogistic> maps : tables.values()) {
                    for (MetaQueueLogistic logic : maps.values()) {
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            }
                            catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }


        public void run() {
            DefaultMetaStore.log.info(this.getServiceName() + " service started");
            int cleanResourceInterval = DefaultMetaStore.this.getMetaStoreConfig().getCleanResourceInterval();
            while (!this.isStoped()) {
                try {
                    this.deleteExpiredFiles();

                    this.waitForRunning(cleanResourceInterval);
                }
                catch (Exception e) {
                    DefaultMetaStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMetaStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return CleanLogicsFileService.class.getSimpleName();
        }
    }

    /**
     * 逻辑队列刷盘服务
     */
    class FlushLogicQueueService extends ServiceThread {
        private static final int RetryTimesOver = 3;
        private long lastFlushTimestamp = 0;


        private void doFlush(int retryTimes) {
            int flushLogicsQueueLeastPages =
                    DefaultMetaStore.this.getMetaStoreConfig().getFlushLogicsQueueLeastPages();

            if (retryTimes == RetryTimesOver) {
                flushLogicsQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            // 定时刷盘
            int flushLogicsQueueThoroughInterval =
                    DefaultMetaStore.this.getMetaStoreConfig().getFlushLogicsQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushLogicsQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushLogicsQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMetaStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentHashMap<String, ConcurrentHashMap<Integer, MetaQueueLogistic>> tables =
                    DefaultMetaStore.this.metaQueueLogisticTable;

            for (ConcurrentHashMap<Integer, MetaQueueLogistic> maps : tables.values()) {
                for (MetaQueueLogistic logic : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = logic.commit(flushLogicsQueueLeastPages);
                    }
                }
            }

            if (0 == flushLogicsQueueLeastPages) {
                DefaultMetaStore.this.getStoreCheckpoint().flush(logicsMsgTimestamp);
            }
        }


        public void run() {
            DefaultMetaStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    int interval = DefaultMetaStore.this.getMetaStoreConfig().getFlushIntervalLogics();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                }
                catch (Exception e) {
                    DefaultMetaStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 正常shutdown时，要保证全部刷盘才退出
            this.doFlush(RetryTimesOver);

            DefaultMetaStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return FlushLogicQueueService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    /**
     * 分发消息索引服务
     */
    class DispatchMessageService extends ServiceThread {
        class DispatchRequest {
            private final String topic;
            private final int queueId;
            private final long offset;
            private final int size;
            private final int type;
            private final long storeTimestamp;
            private final long logicOffset;


            public DispatchRequest(String topic, int queueId, long offset, int size, int type,
                    long storeTimestamp, long logicOffset) {
                this.topic = topic;
                this.queueId = queueId;
                this.offset = offset;
                this.size = size;
                this.type = type;
                this.storeTimestamp = storeTimestamp;
                this.logicOffset = logicOffset;
            }


            public DispatchRequest(int size) {
                this.topic = "";
                this.queueId = 0;
                this.offset = 0;
                this.size = size;
                this.type = 0;
                this.storeTimestamp = 0;
                this.logicOffset = 0;
            }


            public String getTopic() {
                return topic;
            }


            public int getQueueId() {
                return queueId;
            }


            public long getOffset() {
                return offset;
            }


            public int getSize() {
                return size;
            }


            public int getType() {
                return type;
            }


            public long getStoreTimestamp() {
                return storeTimestamp;
            }


            public long getLogicOffset() {
                return logicOffset;
            }
        }

        private volatile List<DispatchRequest> requestsWrite;
        private volatile List<DispatchRequest> requestsRead;


        public DispatchMessageService(int putMsgIndexHightWater) {
            putMsgIndexHightWater *= 1.5;
            this.requestsWrite = new ArrayList<DispatchRequest>(putMsgIndexHightWater);
            this.requestsRead = new ArrayList<DispatchRequest>(putMsgIndexHightWater);
        }


        private void swapRequests() {
            List<DispatchRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }


        public void putRequest(String topic, int queueId, long offset, int size, int type, long storeTimestamp,
                long logicOffset) {
            DispatchRequest dispatchRequest =
                    new DispatchRequest(topic, queueId, offset, size, type, storeTimestamp, logicOffset);
            int requestsWriteSize = 0;
            int putMsgIndexHightWater = DefaultMetaStore.this.getMetaStoreConfig().getPutMsgIndexHightWater();
            synchronized (this) {
                this.requestsWrite.add(dispatchRequest);
                requestsWriteSize = this.requestsWrite.size();
                if (!this.hasNotified) {
                    this.hasNotified = true;
                    this.notify();
                }
            }

            DefaultMetaStore.this.getMetaStatsService().setDispatchMaxBuffer(requestsWriteSize);

            if (requestsWriteSize > putMsgIndexHightWater) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Message index buffer size " + requestsWriteSize + " > high water "
                                + putMsgIndexHightWater);
                    }

                    Thread.sleep(1);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }


        private void doDispatch() {
            if (!this.requestsRead.isEmpty()) {
                for (DispatchRequest req : this.requestsRead) {
                    // 将请求发到具体的逻辑队列
                    DefaultMetaStore.this.putIndex(req.topic, req.queueId, req.offset, req.size, req.type,
                        req.storeTimestamp, req.logicOffset);
                }

                this.requestsRead.clear();
            }
        }


        public void run() {
            DefaultMetaStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(0);
                    this.doDispatch();
                }
                catch (Exception e) {
                    DefaultMetaStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 在正常shutdown情况下，要保证所有消息都dispatch
            try {
                Thread.sleep(5 * 1000);
            }
            catch (InterruptedException e) {
                DefaultMetaStore.log.warn("DispatchMessageService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doDispatch();

            DefaultMetaStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }


        @Override
        public String getServiceName() {
            return DispatchMessageService.class.getSimpleName();
        }
    }

    /**
     * SLAVE: 从物理队列Load消息，并分发到各个逻辑队列
     */
    class ReputMessageService extends ServiceThread {
        // 从这里开始解析物理队列数据，并分发到逻辑队列
        private volatile long reputFromOffset = 0;


        private void doReput() {
            for (boolean doNext = true; doNext;) {
                SelectMapedBufferResult result = DefaultMetaStore.this.metaQueuePhysical.getData(reputFromOffset);
                if (result != null) {
                    try {
                        for (int readSize = 0; readSize < result.getSize() && doNext;) {
                            DispatchRequest dispatchRequest =
                                    DefaultMetaStore.this.metaQueuePhysical.checkMessageAndReturnSize(
                                        result.getByteBuffer(), false, false);
                            int size = dispatchRequest.getSize();
                            // 正常数据
                            if (size > 0) {
                                DefaultMetaStore.this.putDispatchRequest(dispatchRequest.getTopic(),
                                    dispatchRequest.getQueueId(), dispatchRequest.getOffset(),
                                    dispatchRequest.getSize(), dispatchRequest.getType(),
                                    dispatchRequest.getStoreTimestamp(), dispatchRequest.getLogicOffset());

                                this.reputFromOffset += size;
                                readSize += size;
                                DefaultMetaStore.this.metaStatsService.getPutMessageTimesTotal().incrementAndGet();
                                DefaultMetaStore.this.metaStatsService.getPutMessageSizeTotal().addAndGet(
                                    dispatchRequest.getSize());
                            }
                            // 文件中间读到错误
                            else if (size == -1) {
                                doNext = false;
                            }
                            // 走到文件末尾，切换至下一个文件
                            else if (size == 0) {
                                this.reputFromOffset =
                                        DefaultMetaStore.this.metaQueuePhysical.rollNextFile(this.reputFromOffset);
                                readSize = result.getSize();
                            }
                        }
                    }
                    finally {
                        result.release();
                    }
                }
                else {
                    doNext = false;
                }
            }
        }


        @Override
        public void run() {
            DefaultMetaStore.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(1000);
                    this.doReput();
                }
                catch (Exception e) {
                    DefaultMetaStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMetaStore.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }


        public long getReputFromOffset() {
            return reputFromOffset;
        }


        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }
    }


    public void truncateDirtyLogicFiles(long phyOffet) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, MetaQueueLogistic>> tables =
                DefaultMetaStore.this.metaQueueLogisticTable;

        for (ConcurrentHashMap<Integer, MetaQueueLogistic> maps : tables.values()) {
            for (MetaQueueLogistic logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffet);
            }
        }
    }


    private void recover(final boolean lastExitOK) {
        // 先按照正常流程恢复逻辑队列
        this.recoverLogics();

        // 正常数据恢复
        if (lastExitOK) {
            this.metaQueuePhysical.recoverNormally();
        }
        // 异常数据恢复，OS CRASH或者JVM CRASH或者机器掉电
        else {
            this.metaQueuePhysical.recoverAbnormally();
        }

        this.recoverTopicQueueTable();
    }


    /**
     * 加载数据
     * 
     * @throws IOException
     */
    public boolean load() {
        boolean result = false;

        try {
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown " + (lastExitOK ? "normally" : "abnormally"));

            // load 物理队列
            result = this.metaQueuePhysical.load();

            // load 逻辑队列
            result = result && this.loadLogics();

            if (result) {
                this.storeCheckpoint = new StoreCheckpoint(this.metaStoreConfig.getStoreCheckpoint());
                // 尝试恢复数据
                this.recover(lastExitOK);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        if (!result) {
            this.allocateMapedFileService.shutdown();
        }

        return result;
    }


    private boolean loadLogics() {
        File dirLogic = new File(this.metaStoreConfig.getStorePathLogics());
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {
            // TOPIC 遍历
            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();
                // TOPIC 下队列遍历
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId = Integer.parseInt(fileQueueId.getName());
                        MetaQueueLogistic logic = new MetaQueueLogistic(this, topic, queueId);
                        this.putLogicQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }


    private void putLogicQueue(final String topic, final int queueId, final MetaQueueLogistic logic) {
        ConcurrentHashMap<Integer/* queueId */, MetaQueueLogistic> map = this.metaQueueLogisticTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, MetaQueueLogistic>();
            map.put(queueId, logic);
            this.metaQueueLogisticTable.put(topic, map);
        }
        else {
            map.put(queueId, logic);
        }
    }


    /**
     * 启动服务后，在存储根目录创建临时文件，类似于 UNIX VI编辑工具
     * 
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = this.metaStoreConfig.getAbortFile();
        File file = new File(fileName);
        MapedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }


    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }


    private boolean isTempFileExist() {
        String fileName = this.metaStoreConfig.getAbortFile();
        File file = new File(fileName);
        return file.exists();
    }


    /**
     * 启动存储服务
     * 
     * @throws Exception
     */
    public void start() throws Exception {
        this.cleanPhysicFileService.start();
        this.cleanLogicsFileService.start();
        this.dispatchMessageService.start();
        this.flushLogicQueueService.start();
        this.metaQueuePhysical.start();
        this.metaStatsService.start();

        if (!this.metaStoreConfig.isMaster()) {
            this.reputMessageService.setReputFromOffset(this.metaQueuePhysical.getMaxOffset());
            this.reputMessageService.start();
        }
        this.createTempFile();
        this.shutdown = false;
    }


    /**
     * 关闭存储服务
     */
    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            try {
                // 等待其他调用停止
                Thread.sleep(1000 * 3);
            }
            catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }
            this.metaStatsService.shutdown();
            this.cleanPhysicFileService.shutdown();
            this.cleanLogicsFileService.shutdown();
            this.dispatchMessageService.shutdown();
            this.flushLogicQueueService.shutdown();
            this.metaQueuePhysical.shutdown();
            this.allocateMapedFileService.shutdown();
            if (!this.metaStoreConfig.isMaster()) {
                this.reputMessageService.shutdown();
            }
            this.storeCheckpoint.shutdown();
            this.deleteFile(this.metaStoreConfig.getAbortFile());
        }
    }


    public PutMessageResult putMessage(MetaMessage msg, MetaMessageAnnotation msgant) {
        if (this.shutdown) {
            log.warn("meta store has shutdown, so putMessage is forbidden");
            return null;
        }

        if (!this.metaStoreConfig.isMaster()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("meta store is slave mode, so putMessage is forbidden ");
            }

            return null;
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("meta store is not writeable, so putMessage is forbidden "
                        + this.runningFlags.getFlagBits());
            }

            return null;
        }
        else {
            this.printTimes.set(0);
        }

        // message topic长度校验
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return null;
        }

        // message type长度校验
        if (msg.getType() != null && msg.getType().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message type length too long " + msg.getType().length());
            return null;
        }

        // message attribute长度校验
        if (msg.getAttribute() != null && msg.getAttribute().length() > Short.MAX_VALUE) {
            log.warn("putMessage message attribute length too long " + msg.getAttribute().length());
            return null;
        }

        long beginTime = this.getSystemClock().now();
        AppendMessageResult result = this.metaQueuePhysical.putMessage(msg, msgant);
        // 性能数据统计
        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 1000) {
            log.warn("putMessage not in lock eclipse time(ms) " + eclipseTime);
        }
        this.metaStatsService.setPutMessageEntireTimeMax(eclipseTime);
        this.metaStatsService.getPutMessageTimesTotal().incrementAndGet();

        if (null == result || !result.isOk()) {
            this.metaStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return new PutMessageResult(result);
    }


    private boolean isTheBatchFull(long offsetPy, int sizePy, int maxSize, int bufferTotal, int messageTotal) {
        long maxOffsetPy = this.metaQueuePhysical.getMaxOffset();
        long memory = this.metaStoreConfig.getTotalPhysicMemory() * 1024L * 1024L * 1024L;

        // 第一条消息可以不做限制
        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if ((bufferTotal + sizePy) > maxSize) {
            return true;
        }

        // 消息在磁盘
        if ((maxOffsetPy - offsetPy) > memory) {
            if ((bufferTotal + sizePy) > this.metaStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            if ((messageTotal + 1) > this.metaStoreConfig.getMaxTransferCountOnMessageInDisk()) {
                return true;
            }
        }
        // 消息在内存
        else {
            if ((bufferTotal + sizePy) > this.metaStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if ((messageTotal + 1) > this.metaStoreConfig.getMaxTransferCountOnMessageInMemory()) {
                return true;
            }
        }

        return false;
    }


    public GetMessageResult getMessage(String topic, int queueId, long offset, int maxSize, Set<Integer> types) {
        if (this.shutdown) {
            log.warn("meta store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("meta store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();

        // 枚举变量，取消息结果
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        // 当被过滤后，返回下一次开始的Offset
        long nextBeginOffset = 0;
        // 逻辑队列中的最小Offset
        long minOffset = 0;
        // 逻辑队列中的最大Offset
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        MetaQueueLogistic logicQueue = findMetaQueueLogistic(topic, queueId);
        if (logicQueue != null) {
            minOffset = logicQueue.getMinOffsetInQuque();
            maxOffset = logicQueue.getMaxOffsetInQuque();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
            }
            else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
            }
            else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
            }
            else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
            }
            else {
                SelectMapedBufferResult bufferLogic = logicQueue.getIndexBuffer(offset);
                if (bufferLogic != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;

                        int i = 0;
                        final int MaxFilterMessageCount = 16000;
                        for (; i < bufferLogic.getSize() && i < MaxFilterMessageCount; i +=
                                MetaQueueLogistic.StoreUnitSize) {
                            long offsetPy = bufferLogic.getByteBuffer().getLong();
                            int sizePy = bufferLogic.getByteBuffer().getInt();
                            int typeLogic = bufferLogic.getByteBuffer().getInt();

                            // 说明物理文件正在被删除
                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            // 此批消息达到上限了
                            if (this.isTheBatchFull(offsetPy, sizePy, maxSize, getResult.getBufferTotalSize(),
                                getResult.getMessageCount())) {
                                break;
                            }

                            // 消息过滤
                            if (this.messageFilter.isMessageMatched(types, typeLogic)) {
                                SelectMapedBufferResult selectResult =
                                        this.metaQueuePhysical.getMessage(offsetPy, sizePy);
                                if (selectResult != null) {
                                    this.metaStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                                    getResult.addMessage(selectResult);
                                    status = GetMessageStatus.FOUND;
                                    nextPhyFileStartOffset = Long.MIN_VALUE;
                                }
                                else {
                                    if (getResult.getBufferTotalSize() == 0) {
                                        status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                    }

                                    // 物理文件正在被删除，尝试跳过
                                    nextPhyFileStartOffset = this.metaQueuePhysical.rollNextFile(offsetPy);
                                }
                            }
                            else {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                if (log.isDebugEnabled()) {
                                    log.debug("message type not matched, client: " + types + " server: "
                                            + typeLogic);
                                }
                            }
                        }

                        nextBeginOffset = offset + (i / MetaQueueLogistic.StoreUnitSize);
                    }
                    finally {
                        // 必须释放资源
                        bufferLogic.release();
                    }
                }
                else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = logicQueue.rollNextFile(offset);
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset
                            + " maxOffset: " + maxOffset + ", but access logic queue failed.");
                }
            }
        }
        // 请求的队列Id没有
        else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
        }

        if (GetMessageStatus.FOUND == status) {
            this.metaStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        }
        else {
            this.metaStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long eclipseTime = this.getSystemClock().now() - beginTime;
        this.metaStatsService.setGetMessageEntireTimeMax(eclipseTime);

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }


    private MetaQueueLogistic findMetaQueueLogistic(String topic, int queueId) {
        ConcurrentHashMap<Integer, MetaQueueLogistic> map = metaQueueLogisticTable.get(topic);
        if (null == map) {
            ConcurrentHashMap<Integer, MetaQueueLogistic> newMap =
                    new ConcurrentHashMap<Integer, MetaQueueLogistic>(128);
            ConcurrentHashMap<Integer, MetaQueueLogistic> oldMap =
                    metaQueueLogisticTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            }
            else {
                map = newMap;
            }
        }

        MetaQueueLogistic logic = map.get(queueId);
        if (null == logic) {
            MetaQueueLogistic newLogic = new MetaQueueLogistic(this, topic, queueId);
            MetaQueueLogistic oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            }
            else {
                logic = newLogic;
            }
        }

        return logic;
    }


    public void putIndex(String topic, int queueId, long offset, int size, int type, long storeTimestamp,
            long logicOffset) {
        final int MaxRetries = 5;
        boolean canWrite = this.runningFlags.isWriteable();
        for (int i = 0; i < MaxRetries && canWrite; i++) {
            boolean result = this.findMetaQueueLogistic(topic, queueId).putIndex(offset, size, type, logicOffset);
            if (result) {
                this.getStoreCheckpoint().setLogicsMsgTimestamp(storeTimestamp);
                return;
            }
            // 只有一种情况会失败，创建新的MapedFile时报错或者超时
            else {
                log.warn("put index to " + topic + ":" + queueId + " " + offset + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000 * 5);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        this.runningFlags.makeLogicsQueueError();
    }


    public void putDispatchRequest(String topic, int queueId, long offset, int size, int type,
            long storeTimestamp, long logicOffset) {
        this.dispatchMessageService.putRequest(topic, queueId, offset, size, type, storeTimestamp, logicOffset);
    }


    public MetaStoreConfig getMetaStoreConfig() {
        return metaStoreConfig;
    }


    public MetaQueuePhysical getMetaQueuePhysical() {
        return metaQueuePhysical;
    }


    public DispatchMessageService getDispatchMessageService() {
        return dispatchMessageService;
    }


    /**
     * 返回的是当前队列有效的最大Offset，这个Offset有对应的消息
     */
    public long getMaxOffsetInQuque(String topic, int queueId) {
        MetaQueueLogistic logic = this.findMetaQueueLogistic(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQuque();
            //
            // if (offset > 0)
            // offset -= 1;
            return offset;
        }

        return 0;
    }


    public long getMinOffsetInQuque(String topic, int queueId) {
        MetaQueueLogistic logic = this.findMetaQueueLogistic(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQuque();
        }

        return -1;
    }


    public AllocateMapedFileService getAllocateMapedFileService() {
        return allocateMapedFileService;
    }


    public MetaStatsService getMetaStatsService() {
        return metaStatsService;
    }


    public String getRunningDataInfo() {
        return this.metaStatsService.toString();
    }


    public RunningFlags getAccessRights() {
        return runningFlags;
    }


    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        MetaQueueLogistic logic = this.findMetaQueueLogistic(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }


    public MetaMessageWrapper lookMessageByOffset(long phyOffset) {
        SelectMapedBufferResult sbr = this.metaQueuePhysical.getMessage(phyOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(phyOffset, size);
            }
            finally {
                sbr.release();
            }
        }

        return null;
    }


    public MetaMessageWrapper lookMessageByOffset(long phyOffset, int size) {
        SelectMapedBufferResult sbr = this.metaQueuePhysical.getMessage(phyOffset, size);
        if (null != sbr) {
            try {
                return MetaMessageDecoder.decode(sbr.getByteBuffer());
            }
            finally {
                sbr.release();
            }
        }

        return null;
    }


    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, MetaQueueLogistic>> getMetaQueueLogisticTable() {
        return metaQueueLogisticTable;
    }


    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }


    private void recoverLogics() {
        for (ConcurrentHashMap<Integer, MetaQueueLogistic> maps : this.metaQueueLogisticTable.values()) {
            for (MetaQueueLogistic logic : maps.values()) {
                logic.recover();
            }
        }
    }


    private void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.metaQueuePhysical.getMinOffset();
        for (ConcurrentHashMap<Integer, MetaQueueLogistic> maps : this.metaQueueLogisticTable.values()) {
            for (MetaQueueLogistic logic : maps.values()) {
                // 恢复写入消息时，记录的队列offset
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQuque());
                // 恢复每个队列的最小offset
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.metaQueuePhysical.setTopicQueueTable(table);
    }


    public void destroyLogics() {
        for (ConcurrentHashMap<Integer, MetaQueueLogistic> maps : this.metaQueueLogisticTable.values()) {
            for (MetaQueueLogistic logic : maps.values()) {
                logic.destroy();
            }
        }
    }


    public void destroy() {
        this.destroyLogics();
        this.metaQueuePhysical.destroy();
        this.deleteFile(this.metaStoreConfig.getAbortFile());
        this.deleteFile(this.metaStoreConfig.getStoreCheckpoint());
    }


    @Override
    public long getMaxPhyOffset() {
        return this.metaQueuePhysical.getMaxOffset();
    }


    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        MetaQueueLogistic logicQueue = this.findMetaQueueLogistic(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMapedBufferResult result =
                    logicQueue.getIndexBuffer(minLogicOffset / MetaQueueLogistic.StoreUnitSize);
            if (result != null) {
                try {
                    final long phyOffset = result.getByteBuffer().getLong();
                    final int size = result.getByteBuffer().getInt();
                    long storeTime = this.getMetaQueuePhysical().pickupStoretimestamp(phyOffset, size);
                    return storeTime;
                }
                catch (Exception e) {
                }
                finally {
                    result.release();
                }
            }
        }

        return -1;
    }


    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        MetaQueueLogistic logicQueue = this.findMetaQueueLogistic(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }


    @Override
    public SelectMapedBufferResult getPhyQueueData(final long offset) {
        if (this.shutdown) {
            log.warn("meta store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.metaQueuePhysical.getData(offset);
    }


    @Override
    public boolean appendToPhyQueue(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("meta store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.metaQueuePhysical.appendData(startOffset, data);
        if (result) {
            this.reputMessageService.wakeup();
        }
        else {
            log.fatal("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }


    public SystemClock getSystemClock() {
        return systemClock;
    }


    @Override
    public void excuteDeleteFilesManualy() {
        this.cleanPhysicFileService.excuteDeleteFilesManualy();
    }
}

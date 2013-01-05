/**
 * $Id: MetaQueuePhysical.java 3 2013-01-05 08:20:46Z shijia $
 */
package com.taobao.metaq.store;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.taobao.metaq.commons.MetaMessage;
import com.taobao.metaq.commons.MetaMessageAnnotation;
import com.taobao.metaq.commons.MetaMessageDecoder;
import com.taobao.metaq.commons.MetaMessageWrapper;
import com.taobao.metaq.commons.MetaUtil;
import com.taobao.metaq.commons.ServiceThread;
import com.taobao.metaq.store.DefaultMetaStore.DispatchMessageService.DispatchRequest;


/**
 * 存储层物理队列
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class MetaQueuePhysical {
    private static final Logger log = Logger.getLogger(MetaStore.MetaStoreLogName);
    // 用来保存每个逻辑队列的当前最大Offset信息
    private HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);
    // 存储消息的队列
    private final MapedFileQueue mapedFileQueue;
    // 存储顶层对象
    private final DefaultMetaStore defaultMetaStore;
    // 物理队列刷盘服务
    private final FlushPhysicalQueueService flushPhysicalQueueService;
    // 每个消息对应的MAGIC CODE daa320a7
    private final static int MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8;
    // 文件末尾空洞对应的MAGIC CODE cbd43194
    private final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;
    // 存储消息时的回调接口
    private final AppendMessageCallback appendMessageCallback;

    abstract class FlushPhysicalQueueService extends ServiceThread {
    }

    /**
     * 异步实时刷盘服务
     */
    class FlushRealTimeService extends FlushPhysicalQueueService {
        private static final int RetryTimesOver = 3;
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;


        private void printFlushProgress() {
            MetaQueuePhysical.log.info("how much disk fall behind memory, "
                    + MetaQueuePhysical.this.mapedFileQueue.howMuchFallBehind());
        }


        public void run() {
            MetaQueuePhysical.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                int interval =
                        MetaQueuePhysical.this.defaultMetaStore.getMetaStoreConfig().getFlushIntervalPhysic();
                int flushPhysicQueueLeastPages =
                        MetaQueuePhysical.this.defaultMetaStore.getMetaStoreConfig()
                            .getFlushPhysicQueueLeastPages();

                int flushPhysicQueueThoroughInterval =
                        MetaQueuePhysical.this.defaultMetaStore.getMetaStoreConfig()
                            .getFlushPhysicQueueThoroughInterval();

                boolean printFlushProgress = false;

                // 定时刷盘，定时打印刷盘进度
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = ((printTimes++ % 10) == 0);
                }

                try {
                    this.waitForRunning(interval);

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    MetaQueuePhysical.this.mapedFileQueue.commit(flushPhysicQueueLeastPages);
                    long storeTimestamp = MetaQueuePhysical.this.mapedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        MetaQueuePhysical.this.defaultMetaStore.getStoreCheckpoint().setPhysicMsgTimestamp(
                            storeTimestamp);
                    }
                }
                catch (Exception e) {
                    MetaQueuePhysical.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // 正常shutdown时，要保证全部刷盘才退出
            boolean result = false;
            for (int i = 0; i < RetryTimesOver && !result; i++) {
                result = MetaQueuePhysical.this.mapedFileQueue.commit(0);
                MetaQueuePhysical.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1)
                        + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            MetaQueuePhysical.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return FlushPhysicalQueueService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            // 由于物理队列数据量较大，所以回收时间要更长
            return 1000 * 60 * 5;
        }
    }

    class GroupCommitRequest {
        // 当前消息对应的下一个Offset
        private final long nextOffset;
        // 异步通知对象
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        // 刷盘是否成功
        private volatile boolean flushOK = false;


        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }


        public long getNextOffset() {
            return nextOffset;
        }


        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }


        public boolean waitForFlush(long timeout) {
            try {
                boolean result = this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return result || this.flushOK;
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    /**
     * GroupCommit Service
     */
    class GroupCommitService extends FlushPhysicalQueueService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();


        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }


        public void putRequest(final GroupCommitRequest request) {
            synchronized (this) {
                this.requestsWrite.add(request);
                if (!this.hasNotified) {
                    this.hasNotified = true;
                    this.notify();
                }
            }
        }


        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    // 消息有可能在下一个文件，所以最多刷盘2次
                    for (int i = 0; (i < 2)
                            && (MetaQueuePhysical.this.mapedFileQueue.getCommittedWhere() < req.getNextOffset()); i++) {
                        MetaQueuePhysical.this.mapedFileQueue.commit(0);
                    }

                    req.wakeupCustomer(true);
                }

                long storeTimestamp = MetaQueuePhysical.this.mapedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    MetaQueuePhysical.this.defaultMetaStore.getStoreCheckpoint().setPhysicMsgTimestamp(
                        storeTimestamp);
                }

                this.requestsRead.clear();
            }
        }


        public void run() {
            MetaQueuePhysical.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(0);
                    this.doCommit();
                }
                catch (Exception e) {
                    MetaQueuePhysical.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 在正常shutdown情况下，等待请求到来，然后再刷盘
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                MetaQueuePhysical.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            MetaQueuePhysical.log.info(this.getServiceName() + " service end");
        }


        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }


        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            // 由于物理队列数据量较大，所以回收时间要更长
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // 存储消息ID
        private final ByteBuffer msgIdMemory;
        // 存储消息内容
        private final ByteBuffer msgStoreItemMemory;
        // 消息的最大长度
        private final int maxMessageSize;

        // 文件末尾空洞最小定长
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;


        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MetaMessageDecoder.MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }


        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }


        private void resetMsgStoreItemMemory(final int length) {
            this.msgStoreItemMemory.flip();
            this.msgStoreItemMemory.limit(length);
        }


        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                final int maxBlank, final Object msg) {
            /**
             * 生成消息ID STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>
             */
            MetaMessageWrapper wrapper = (MetaMessageWrapper) msg;
            // PHY OFFSET
            long wroteOffset = fileFromOffset + byteBuffer.position();
            String msgId =
                    MetaMessageDecoder.createMessageId(this.msgIdMemory, (int) (wrapper.getMetaMessageAnnotation()
                        .getStoreTimestamp() / 1000), wrapper.getMetaMessageAnnotation().getStoreHostBytes(),
                        wroteOffset);

            /**
             * 记录逻辑队列信息
             */
            String key =
                    wrapper.getMetaMessage().getTopic() + "-" + wrapper.getMetaMessageAnnotation().getQueueId();
            Long queueOffset = MetaQueuePhysical.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                MetaQueuePhysical.this.topicQueueTable.put(key, queueOffset);
            }

            /**
             * 序列化消息
             */
            int attributeLength =
                    wrapper.getMetaMessage().getAttribute() == null ? 0 : wrapper.getMetaMessage().getAttribute()
                        .length();
            int bodyLength =
                    wrapper.getMetaMessage().getBody() == null ? 0 : wrapper.getMetaMessage().getBody().length;

            int msgLen = 4 // 1 TOTALSIZE
                    + 4 // 2 MAGICCODE
                    + 4 // 3 BODYCRC
                    + 4 // 4 QUEUEID
                    + 4 // 5 FLAG
                    + 8 // 6 QUEUEOFFSET
                    + 8 // 7 PHYSICALOFFSET
                    + 4 // 8 SYSFLAG
                    + 8 // 9 BORNTIMESTAMP
                    + 8 // 10 BORNHOST
                    + 8 // 11 STORETIMESTAMP
                    + 8 // 12 STOREHOSTADDRESS
                    + 8 // 13 REQUESTID（暂不使用，为同步双写准备）
                    + 1 + wrapper.getMetaMessage().getTopic().length() // 14TOPIC
                    + 1 + wrapper.getMetaMessage().getType().length() // 15TYPE
                    + 2 + attributeLength // 16 ATTRIBUTE
                    + 4 + bodyLength // 17 BODY
                    + 0;

            // 消息超过设定的最大值
            if (msgLen > this.maxMessageSize) {
                MetaQueuePhysical.log.warn("message size exceeded, msg total size: " + msgLen
                        + ", msg body size: " + bodyLength + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // 判断是否有足够空余空间
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetMsgStoreItemMemory(maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(MetaQueuePhysical.BlankMagicCode);
                // 3 剩余空间可能是任何值
                //

                // 此处长度特意设置为maxBlank
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId,
                    wrapper.getMetaMessageAnnotation().getStoreTimestamp(), queueOffset);
            }

            // 初始化存储空间
            this.resetMsgStoreItemMemory(msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(MetaQueuePhysical.MessageMagicCode);
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(wrapper.getMetaMessageAnnotation().getBodyCRC());
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(wrapper.getMetaMessageAnnotation().getQueueId());
            // 5 FLAG
            this.msgStoreItemMemory.putInt(wrapper.getMetaMessage().getFlag());
            // 6 QUEUEOFFSET
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG
            this.msgStoreItemMemory.putInt(wrapper.getMetaMessageAnnotation().getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(wrapper.getMetaMessageAnnotation().getBornTimestamp());
            // 10 BORNHOST
            this.msgStoreItemMemory.put(wrapper.getMetaMessageAnnotation().getBornHostBytes());
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(wrapper.getMetaMessageAnnotation().getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            this.msgStoreItemMemory.put(wrapper.getMetaMessageAnnotation().getStoreHostBytes());
            // 13 REQUESTID
            this.msgStoreItemMemory.putLong(0L);
            // 14 TOPIC
            this.msgStoreItemMemory.put((byte) wrapper.getMetaMessage().getTopic().length());
            this.msgStoreItemMemory.put(wrapper.getMetaMessage().getTopic().getBytes());
            // 15 TYPE
            this.msgStoreItemMemory.put((byte) wrapper.getMetaMessage().getType().length());
            this.msgStoreItemMemory.put(wrapper.getMetaMessage().getType().getBytes());
            // 16 ATTRIBUTE
            this.msgStoreItemMemory.putShort((short) attributeLength);
            if (attributeLength > 0)
                this.msgStoreItemMemory.put(wrapper.getMetaMessage().getAttribute().getBytes());
            // 17 BODY
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(wrapper.getMetaMessage().getBody());

            // 向队列缓冲区写入消息
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            AppendMessageResult result =
                    new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId, wrapper
                        .getMetaMessageAnnotation().getStoreTimestamp(), queueOffset);

            // 更新下一次的逻辑队列信息
            queueOffset++;
            MetaQueuePhysical.this.topicQueueTable.put(key, queueOffset);

            // 返回结果
            return result;
        }
    }


    /**
     * 构造函数
     */
    public MetaQueuePhysical(final DefaultMetaStore defaultMetaStore) {
        this.mapedFileQueue =
                new MapedFileQueue(defaultMetaStore.getMetaStoreConfig().getStorePathPhysic(), defaultMetaStore
                    .getMetaStoreConfig().getMapedFileSizePhysic(), defaultMetaStore.getAllocateMapedFileService());
        this.defaultMetaStore = defaultMetaStore;

        if (defaultMetaStore.getMetaStoreConfig().isGroupCommitEnable()) {
            this.flushPhysicalQueueService = new GroupCommitService();
        }
        else {
            this.flushPhysicalQueueService = new FlushRealTimeService();
        }

        this.appendMessageCallback =
                new DefaultAppendMessageCallback(defaultMetaStore.getMetaStoreConfig().getMaxMessageSize());
    }


    public boolean load() {
        boolean result = this.mapedFileQueue.load();
        log.info("load physic queue " + (result ? "OK" : "Failed"));
        return result;
    }


    public void start() {
        this.flushPhysicalQueueService.start();
    }


    public void shutdown() {
        this.flushPhysicalQueueService.shutdown();
    }


    public long getMinOffset() {
        MapedFile mapedFile = this.mapedFileQueue.getFirstMapedFileOnLock();
        if (mapedFile != null) {
            if (mapedFile.isAvailable()) {
                return mapedFile.getFileFromOffset();
            }
            else {
                return this.rollNextFile(mapedFile.getFileFromOffset());
            }
        }

        return -1;
    }


    public long getMaxOffset() {
        return this.mapedFileQueue.getMaxOffset();
    }


    public int deleteExpiredFile(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly) {
        return this.mapedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly);
    }


    /**
     * 读取消息
     */
    public SelectMapedBufferResult getMessage(final long offset, final int size) {
        int mapedFileSize = this.defaultMetaStore.getMetaStoreConfig().getMapedFileSizePhysic();
        MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset, (0 == offset ? true : false));
        if (mapedFile != null) {
            int pos = (int) (offset % mapedFileSize);
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer(pos, size);
            return result;
        }

        return null;
    }


    public long rollNextFile(final long offset) {
        int mapedFileSize = this.defaultMetaStore.getMetaStoreConfig().getMapedFileSizePhysic();
        return (offset + mapedFileSize - offset % mapedFileSize);
    }


    /**
     * 读取物理队列数据，数据复制时使用
     */
    public SelectMapedBufferResult getData(final long offset) {
        int mapedFileSize = this.defaultMetaStore.getMetaStoreConfig().getMapedFileSizePhysic();
        MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset, (0 == offset ? true : false));
        if (mapedFile != null) {
            int pos = (int) (offset % mapedFileSize);
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer(pos);
            return result;
        }

        return null;
    }


    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }


    /**
     * 服务端使用 检查消息并返回消息大小
     * 
     * @return 0 表示走到文件末尾 >0 正常消息 -1 消息校验失败
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
            final boolean readBody) {
        try {
            java.nio.ByteBuffer byteBufferMessage =
                    ((DefaultAppendMessageCallback) this.appendMessageCallback).getMsgStoreItemMemory();
            byte[] bytesContent = byteBufferMessage.array();

            // 1 TOTALSIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGICCODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
            case MessageMagicCode:
                break;
            case BlankMagicCode:
                return this.defaultMetaStore.getDispatchMessageService().new DispatchRequest(0);
            default:
                log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                return this.defaultMetaStore.getDispatchMessageService().new DispatchRequest(-1);
            }

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();

            // 5 FLAG
            int flag = byteBuffer.getInt();
            flag = flag + 0;

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();
            sysFlag = sysFlag + 0;

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            bornTimeStamp = bornTimeStamp + 0;

            // 10 BORNHOST（IP+PORT）
            byteBuffer.get(bytesContent, 0, 8);

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();

            // 12 STOREHOST（IP+PORT）
            byteBuffer.get(bytesContent, 0, 8);

            // 13 REQUESTID
            long requestId = byteBuffer.getLong();
            requestId = requestId + 0;

            // 14 TOPIC
            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen);

            // 15 TYPE
            byte typeLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, typeLen);
            String type = new String(bytesContent, 0, typeLen);

            // 16 ATTRIBUTE
            short attributeLen = byteBuffer.getShort();
            if (attributeLen > 0) {
                byteBuffer.get(bytesContent, 0, attributeLen);
            }

            // 17 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    // 校验CRC
                    if (checkCRC) {
                        int crc = MetaUtil.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed " + crc + " " + bodyCRC);
                            return this.defaultMetaStore.getDispatchMessageService().new DispatchRequest(-1);
                        }
                    }
                }
                else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            return this.defaultMetaStore.getDispatchMessageService().new DispatchRequest(topic, queueId,
                physicOffset, totalSize, type.hashCode(), storeTimestamp, queueOffset);
        }
        catch (BufferUnderflowException e) {
            byteBuffer.position(byteBuffer.limit());
        }
        catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return this.defaultMetaStore.getDispatchMessageService().new DispatchRequest(-1);
    }


    /**
     * 正常退出时，数据恢复，所有内存数据都已经刷盘
     */
    public void recoverNormally() {
        boolean checkCRCOnRecover = this.defaultMetaStore.getMetaStoreConfig().isCheckCRCOnRecover();
        final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
            // 从倒数第三个文件开始恢复
            int index = mapedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MapedFile mapedFile = mapedFiles.get(index);
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            long processOffset = mapedFile.getFileFromOffset();
            long mapedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getSize();
                // 正常数据
                if (size > 0) {
                    mapedFileOffset += size;
                }
                // 文件中间读到错误
                else if (size == -1) {
                    log.info("recover physics file end, " + mapedFile.getFileName());
                    break;
                }
                // 走到文件末尾，切换至下一个文件
                // 由于返回0代表是遇到了最后的空洞，这个可以不计入truncate offset中
                else if (size == 0) {
                    index++;
                    if (index >= mapedFiles.size()) {
                        // 当前条件分支不可能发生
                        log.info("recover last 3 physics file over, last maped file " + mapedFile.getFileName());
                        break;
                    }
                    else {
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        processOffset = mapedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next physics file, " + mapedFile.getFileName());
                    }
                }
            }

            processOffset += mapedFileOffset;
            this.mapedFileQueue.setCommittedWhere(processOffset);
            this.mapedFileQueue.truncateDirtyFiles(processOffset);
        }
    }


    public void recoverAbnormally() {
        // 根据最小时间戳来恢复
        boolean checkCRCOnRecover = this.defaultMetaStore.getMetaStoreConfig().isCheckCRCOnRecover();
        final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
            // 寻找从哪个文件开始恢复
            int index = mapedFiles.size() - 1;
            MapedFile mapedFile = null;
            for (; index >= 0; index--) {
                mapedFile = mapedFiles.get(index);
                if (this.isMapedFileMatchedRecover(mapedFile)) {
                    log.info("recover from this maped file " + mapedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mapedFile = mapedFiles.get(index);
            }

            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            long processOffset = mapedFile.getFileFromOffset();
            long mapedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getSize();
                // 正常数据
                if (size > 0) {
                    mapedFileOffset += size;
                    this.defaultMetaStore.putIndex(dispatchRequest.getTopic(), dispatchRequest.getQueueId(),
                        dispatchRequest.getOffset(), dispatchRequest.getSize(), dispatchRequest.getType(),
                        dispatchRequest.getStoreTimestamp(), dispatchRequest.getLogicOffset());
                }
                // 文件中间读到错误
                else if (size == -1) {
                    log.info("recover physics file end, " + mapedFile.getFileName());
                    break;
                }
                // 走到文件末尾，切换至下一个文件
                // 由于返回0代表是遇到了最后的空洞，这个可以不计入truncate offset中
                else if (size == 0) {
                    index++;
                    if (index >= mapedFiles.size()) {
                        // 当前条件分支正常情况下不应该发生
                        log.info("recover physics file over, last maped file " + mapedFile.getFileName());
                        break;
                    }
                    else {
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        processOffset = mapedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next physics file, " + mapedFile.getFileName());
                    }
                }
            }

            processOffset += mapedFileOffset;
            this.mapedFileQueue.setCommittedWhere(processOffset);
            this.mapedFileQueue.truncateDirtyFiles(processOffset);

            // 清除逻辑队列的多余数据
            this.defaultMetaStore.truncateDirtyLogicFiles(processOffset);
        }
        // 物理文件都被删除情况下
        else {
            this.mapedFileQueue.setCommittedWhere(0);
            this.defaultMetaStore.destroyLogics();
        }
    }


    private boolean isMapedFileMatchedRecover(final MapedFile mapedFile) {
        ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MetaMessageDecoder.MessageMagicCodePostion);
        if (magicCode != MessageMagicCode) {
            return false;
        }

        long storeTimestamp = byteBuffer.getLong(MetaMessageDecoder.MessageStoreTimestampPostion);
        if (0 == storeTimestamp) {
            return false;
        }

        if (storeTimestamp <= this.defaultMetaStore.getStoreCheckpoint().getMinTimestamp()) {
            return true;
        }

        return false;
    }


    public AppendMessageResult putMessage(final MetaMessage msg, final MetaMessageAnnotation msgant) {
        MetaMessageWrapper metaMessageWrapper = new MetaMessageWrapper(msg, msgant);
        // 设置存储时间
        msgant.setStoreTimestamp(System.currentTimeMillis());
        // 设置消息体BODY CRC（考虑在客户端设置最合适）
        msgant.setBodyCRC(MetaUtil.crc32(msg.getBody()));
        // 返回结果
        AppendMessageResult result = null;

        MetaStatsService metaStatsService = this.defaultMetaStore.getMetaStatsService();

        // 写文件要加锁
        synchronized (this) {
            long beginLockTimestamp = this.defaultMetaStore.getSystemClock().now();

            // 这里设置存储时间戳，才能保证全局有序
            msgant.setStoreTimestamp(beginLockTimestamp);

            // 尝试写入
            MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile();
            if (null == mapedFile) {
                log.fatal("create maped file1 error, topic: " + msg.getTopic() + " clientAddr: "
                        + msgant.getBornHostString());
                return null;
            }
            result = mapedFile.appendMessage(metaMessageWrapper, this.appendMessageCallback);
            switch (result.getStatus()) {
            // 成功追加消息
            case PUT_OK:
                break;
            // 走到文件末尾
            case END_OF_FILE:
                // 创建新文件，重新写消息
                mapedFile = this.mapedFileQueue.getLastMapedFile();
                if (null == mapedFile) {
                    log.fatal("create maped file2 error, topic: " + msg.getTopic() + " clientAddr: "
                            + msgant.getBornHostString());
                    return null;
                }
                result = mapedFile.appendMessage(metaMessageWrapper, this.appendMessageCallback);
                break;
            // 消息大小超限
            case MESSAGE_SIZE_EXCEEDED:
                return result;
                // 未知错误
            case UNKNOWN_ERROR:
                return result;
            default:
                return result;
            }

            // 派发消息位置信息到逻辑队列
            this.defaultMetaStore.putDispatchRequest(msg.getTopic(), msgant.getQueueId(), result.getWroteOffset(),
                result.getWroteBytes(), msg.getType().hashCode(), msgant.getStoreTimestamp(),
                result.getLogicsOffset());

            long eclipseTime = this.defaultMetaStore.getSystemClock().now() - beginLockTimestamp;
            if (eclipseTime > 1000) {
                log.warn("putMessage in lock eclipse time(ms) " + eclipseTime);
            }
        }

        // 统计消息SIZE
        metaStatsService.getPutMessageSizeTotal().addAndGet(result.getWroteBytes());

        // 同步刷盘
        if (this.defaultMetaStore.getMetaStoreConfig().isGroupCommitEnable()) {
            GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
            GroupCommitService service = (GroupCommitService) this.flushPhysicalQueueService;
            service.putRequest(request);
            boolean flushOK =
                    request.waitForFlush(this.defaultMetaStore.getMetaStoreConfig().getGroupCommitTimeout());
            if (!flushOK) {
                log.error("do groupcommit, wait for flush failed, topic: " + msg.getTopic() + " type: "
                        + msg.getType() + " client address: " + msgant.getBornHostString());
            }
        }
        // 异步刷盘
        else {
            this.flushPhysicalQueueService.wakeup();
        }

        // 向发送方返回结果
        return result;
    }


    /**
     * 根据offset获取特定消息的存储时间 如果出错，则返回-1
     */
    public long pickupStoretimestamp(final long offset, final int size) {
        SelectMapedBufferResult result = this.getMessage(offset, size);
        if (null != result) {
            try {
                return result.getByteBuffer().getLong(MetaMessageDecoder.MessageStoreTimestampPostion);
            }
            finally {
                result.release();
            }
        }

        return -1;
    }


    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }


    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }


    public void destroy() {
        this.mapedFileQueue.destroy();
    }


    public boolean appendData(long startOffset, byte[] data) {
        // 写文件要加锁
        synchronized (this) {
            // 尝试写入
            MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile(startOffset);
            if (null == mapedFile) {
                log.fatal("appendData getLastMapedFile error  " + startOffset);
                return false;
            }

            return mapedFile.appendMessage(data);
        }
    }


    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mapedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }
}

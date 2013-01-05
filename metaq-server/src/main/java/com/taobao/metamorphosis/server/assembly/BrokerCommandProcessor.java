package com.taobao.metamorphosis.server.assembly;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.buffer.IoBuffer;
import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RemotingServer;
import com.taobao.gecko.service.SingleRequestCallBackListener;
import com.taobao.gecko.service.exception.NotifyRemotingException;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.network.AskCommand;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.ByteUtils;
import com.taobao.metamorphosis.network.FetchCommand;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.MessageTypeCommand;
import com.taobao.metamorphosis.network.OffsetCommand;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.QuitCommand;
import com.taobao.metamorphosis.network.StatsCommand;
import com.taobao.metamorphosis.network.VersionCommand;
import com.taobao.metamorphosis.server.BrokerZooKeeper;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.server.exception.MetamorphosisException;
import com.taobao.metamorphosis.server.network.PutCallback;
import com.taobao.metamorphosis.server.network.SessionContext;
import com.taobao.metamorphosis.server.stats.StatsManager;
import com.taobao.metamorphosis.server.store.AppendCallback;
import com.taobao.metamorphosis.server.store.Location;
import com.taobao.metamorphosis.server.store.MessageStoreManager;
import com.taobao.metamorphosis.server.transaction.Transaction;
import com.taobao.metamorphosis.server.utils.BuildProperties;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.transaction.TransactionId;
import com.taobao.metamorphosis.transaction.XATransactionId;
import com.taobao.metamorphosis.utils.IdWorker;
import com.taobao.metamorphosis.utils.MessageFlagUtils;
import com.taobao.metamorphosis.utils.MessageUtils;
import com.taobao.metaq.commons.MetaMessage;
import com.taobao.metaq.commons.MetaMessageAnnotation;
import com.taobao.metaq.commons.MetaMessageDecoder;
import com.taobao.metaq.commons.MetaUtil;
import com.taobao.metaq.store.GetMessageResult;
import com.taobao.metaq.store.MetaStore;
import com.taobao.metaq.store.PutMessageResult;
import com.taobao.metaq.store.SelectMapedBufferResult;


/**
 * meta服务端核心处理器
 * 
 * @author boyan
 * 
 */
public class BrokerCommandProcessor implements CommandProcessor {
    /**
     * append到message store的callback
     * 
     * @author boyan(boyan@taobao.com)
     * @date 2011-12-7
     * 
     */
    public final class StoreAppendCallback implements AppendCallback {
        private final int partition;
        private final String partitionString;
        private final PutCommand request;
        private final long messageId;
        private final PutCallback cb;


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + this.getOuterType().hashCode();
            result = prime * result + (this.cb == null ? 0 : this.cb.hashCode());
            result = prime * result + (int) (this.messageId ^ this.messageId >>> 32);
            result = prime * result + this.partition;
            result = prime * result + (this.partitionString == null ? 0 : this.partitionString.hashCode());
            result = prime * result + (this.request == null ? 0 : this.request.hashCode());
            return result;
        }


        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (this.getClass() != obj.getClass()) {
                return false;
            }
            final StoreAppendCallback other = (StoreAppendCallback) obj;
            if (!this.getOuterType().equals(other.getOuterType())) {
                return false;
            }
            if (this.cb == null) {
                if (other.cb != null) {
                    return false;
                }
            }
            else if (!this.cb.equals(other.cb)) {
                return false;
            }
            if (this.messageId != other.messageId) {
                return false;
            }
            if (this.partition != other.partition) {
                return false;
            }
            if (this.partitionString == null) {
                if (other.partitionString != null) {
                    return false;
                }
            }
            else if (!this.partitionString.equals(other.partitionString)) {
                return false;
            }
            if (this.request == null) {
                if (other.request != null) {
                    return false;
                }
            }
            else if (!this.request.equals(other.request)) {
                return false;
            }
            return true;
        }


        public StoreAppendCallback(final int partition, final String partitionString, final PutCommand request,
                final long messageId, final PutCallback cb) {
            this.partition = partition;
            this.partitionString = partitionString;
            this.request = request;
            this.messageId = messageId;
            this.cb = cb;
        }


        @Override
        public void appendComplete(final Location location) {
            final long offset = location.getOffset();
            if (offset != -1) {
                final String resultStr =
                        BrokerCommandProcessor.this.genPutResultString(this.partition, this.messageId, offset);
                if (this.cb != null) {
                    this.cb
                        .putComplete(new BooleanCommand(this.request.getOpaque(), HttpStatus.Success, resultStr));
                }
            }
            else {
                BrokerCommandProcessor.this.statsManager.statsPutFailed(this.request.getTopic(),
                    this.partitionString, 1);
                if (this.cb != null) {
                    this.cb.putComplete(new BooleanCommand(this.request.getOpaque(),
                        HttpStatus.InternalServerError, "put message failed"));
                }
            }

        }


        private BrokerCommandProcessor getOuterType() {
            return BrokerCommandProcessor.this;
        }
    }

    static final Log log = LogFactory.getLog(BrokerCommandProcessor.class);

    protected MessageStoreManager storeManager;
    protected ExecutorsManager executorsManager;
    protected StatsManager statsManager;
    protected RemotingServer remotingServer;
    protected MetaConfig metaConfig;
    protected IdWorker idWorker;
    protected BrokerZooKeeper brokerZooKeeper;
    protected MetaStore metaStore;
    protected MessageTypeManager messageTypeManager;
    protected final boolean tellMaxOffset = Boolean.parseBoolean(System.getProperty("meta.get.tellMaxOffset",
        "false"));


    /**
     * 仅用于测试
     */
    public BrokerCommandProcessor() {
        super();
    }


    public BrokerCommandProcessor(final MessageStoreManager storeManager, final ExecutorsManager executorsManager,
            final StatsManager statsManager, final RemotingServer remotingServer, final MetaConfig metaConfig,
            final IdWorker idWorker, final BrokerZooKeeper brokerZooKeeper, final MetaStore metaStore, final MessageTypeManager messageTypeManager) {
        super();
        this.storeManager = storeManager;
        this.executorsManager = executorsManager;
        this.statsManager = statsManager;
        this.remotingServer = remotingServer;
        this.metaConfig = metaConfig;
        this.idWorker = idWorker;
        this.brokerZooKeeper = brokerZooKeeper;
        this.metaStore = metaStore;
        this.messageTypeManager = messageTypeManager;
    }


    public void setMetaStore(MetaStore metaStore) {
		this.metaStore = metaStore;
	}


	public MetaStore getMetaStore() {
        return this.metaStore;
    }


    public MessageTypeManager getMessageTypeManager() {
		return messageTypeManager;
	}


	public void setMessageTypeManager(MessageTypeManager messageTypeManager) {
		this.messageTypeManager = messageTypeManager;
	}


	public MessageStoreManager getStoreManager() {
        return this.storeManager;
    }


    public void setStoreManager(final MessageStoreManager storeManager) {
        this.storeManager = storeManager;
    }


    public ExecutorsManager getExecutorsManager() {
        return this.executorsManager;
    }


    public void setExecutorsManager(final ExecutorsManager executorsManager) {
        this.executorsManager = executorsManager;
    }


    public StatsManager getStatsManager() {
        return this.statsManager;
    }


    public void setStatsManager(final StatsManager statsManager) {
        this.statsManager = statsManager;
    }


    public RemotingServer getRemotingServer() {
        return this.remotingServer;
    }


    public void setRemotingServer(final RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }


    public MetaConfig getMetaConfig() {
        return this.metaConfig;
    }


    public void setMetaConfig(final MetaConfig metaConfig) {
        this.metaConfig = metaConfig;
    }


    public IdWorker getIdWorker() {
        return this.idWorker;
    }


    public void setIdWorker(final IdWorker idWorker) {
        this.idWorker = idWorker;
    }


    public BrokerZooKeeper getBrokerZooKeeper() {
        return this.brokerZooKeeper;
    }


    public void setBrokerZooKeeper(final BrokerZooKeeper brokerZooKeeper) {
        this.brokerZooKeeper = brokerZooKeeper;
    }


    @Override
    public void init() {

    }


    @Override
    public void dispose() {

    }


    @Override
    public void processPutCommand(final PutCommand request, final SessionContext sessionContext,
            final PutCallback cb) {
        final String partitionString = this.metaConfig.getBrokerId() + "-" + request.getPartition();
        this.statsManager.statsPut(request.getTopic(), partitionString, 1);
        this.statsManager.statsMessageSize(request.getTopic(), request.getData().length);
        try {
            if (this.metaConfig.isClosedPartition(request.getTopic(), request.getPartition())) {
                log.warn("Can not put message to partition " + request.getPartition() + " for topic="
                        + request.getTopic() + ",it was closed");
                if (cb != null) {
                    cb.putComplete(new BooleanCommand(request.getOpaque(), HttpStatus.Forbidden, "Partition["
                            + partitionString + "] has been closed"));
                }
                return;
            }

            // 如果是动态添加的topic，需要注册到zk
            this.brokerZooKeeper.registerTopicInZk(request.getTopic());

            // 2.0存储
            MetaStore metaStore = this.getMetaStore();
            MetaMessage message =
            // Flag在服务器中强制增加NewServerFlag标记位，为consumer兼容准备
                    new MetaMessage(request.getTopic(), "", "", request.getFlag()
                            | MetaMessageDecoder.NewServerFlag, request.getData());
            // 为了兼容META1.4版本，对attribute进行特殊处理
            // 如果有属性，需要解析属性
            if (MessageFlagUtils.hasAttribute(request.getFlag())) {
                // 取4个字节的属性长度
                final int attrLen = MessageUtils.getInt(0, request.getData());
                // 取消息属性
                final byte[] attrData = new byte[attrLen];
                System.arraycopy(request.getData(), 4, attrData, 0, attrLen);
                String attribute = ByteUtils.getString(attrData);
                message.setAttribute(attribute);

                // 暂时将消息属性当做消息类型来处理
                message.setType(attribute);

                int bodyLen = request.getData().length - (4 + attrLen);
                final byte[] bodyData = new byte[bodyLen];
                System.arraycopy(request.getData(), 4 + attrLen, bodyData, 0, bodyLen);
                message.setBody(bodyData);
            }
            int partitionId=this.getPartition(request);
            MetaMessageAnnotation msgant = new MetaMessageAnnotation();
            msgant.setQueueId(partitionId);
            msgant.setSysFlag(0);
            msgant.setBornTimestamp(System.currentTimeMillis());
            SocketAddress bornHost = (null!=sessionContext ? 
            		sessionContext.getConnection().getRemoteSocketAddress() : 
            			new InetSocketAddress((InetAddress)null, 8123));
            msgant.setBornHost(bornHost);
            
            msgant.setStoreTimestamp(System.currentTimeMillis());
            
            SocketAddress storeHost = (null!=sessionContext ? 
            		new InetSocketAddress(sessionContext.getConnection().getLocalAddress(),
            				this.metaConfig.getServerPort()) : 
            			new InetSocketAddress((InetAddress)null, 8123));
            msgant.setStoreHost(storeHost);
            
            msgant.setBodyCRC(MetaUtil.crc32(request.getData()));

            PutMessageResult result = metaStore.putMessage(message, msgant);
            if (result != null && result.isOk()) {
                if (cb != null) {

                    // 1 1.4版本消息ID
                    String resultStr = String.valueOf(result.getAppendMessageResult().getWroteOffset());
                    resultStr += " ";
                    // 2 分区信息
                    resultStr += String.valueOf(partitionId);
                    resultStr += " ";
                    // 3 物理分区offset
                    resultStr += String.valueOf(result.getAppendMessageResult().getWroteOffset());
                    resultStr += " ";
                    // 4 2.0版本消息ID
                    resultStr += result.getAppendMessageResult().getMsgId();

                    cb.putComplete(new BooleanCommand(request.getOpaque(), HttpStatus.Success, resultStr));
                }
            }
            else {
                cb.putComplete(new BooleanCommand(request.getOpaque(), HttpStatus.InternalServerError,
                    "put message failed"));
            }

        }
        catch (final Exception e) {
            this.statsManager.statsPutFailed(request.getTopic(), partitionString, 1);
            log.error("Put message failed", e);
            if (cb != null) {
                cb.putComplete(new BooleanCommand(request.getOpaque(), HttpStatus.InternalServerError, e
                    .getMessage()));
            }
        }
    }


    protected int getPartition(final PutCommand request) {
        int partition = request.getPartition();
        if (partition == Partition.RandomPartiton.getPartition()) {
            partition = this.storeManager.chooseRandomPartition(request.getTopic());
        }
        return partition;
    }


    /**
     * 返回形如"messageId partition offset"的字符号，返回给客户端
     * 
     * @param partition
     * @param messageId
     * @param offset
     * @return
     */
    protected String genPutResultString(final int partition, final long messageId, final long offset) {
        final StringBuilder sb =
                new StringBuilder(ByteUtils.stringSize(offset) + ByteUtils.stringSize(messageId)
                        + ByteUtils.stringSize(partition) + 2);
        sb.append(messageId).append(" ").append(partition).append(" ").append(offset);
        return sb.toString();
    }


    @Override
    public ResponseCommand processGetCommand(final GetCommand request, final SessionContext ctx) {
        return this.processGetCommand(request, ctx, true);
    }


    private IoBuffer makeHead(final int opaque, final long size) {
        final IoBuffer buf = IoBuffer.allocate(9 + ByteUtils.stringSize(opaque) + ByteUtils.stringSize(size));
        ByteUtils.setArguments(buf, "value", size, opaque);
        buf.flip();
        return buf;
    }


    private void writePageCache(final GetCommand request, final SessionContext ctx,
            final GetMessageResult getMessageResult) {
        final IoBuffer head = this.makeHead(request.getOpaque(), getMessageResult.getBufferTotalSize());

        SingleRequestCallBackListener listener = new SingleRequestCallBackListener() {

            @Override
            public void onResponse(ResponseCommand responseCommand, Connection conn) {
                getMessageResult.release();

            }


            @Override
            public void onException(Exception e) {
                getMessageResult.release();
            }


            @Override
            public ThreadPoolExecutor getExecutor() {
                return null;
            }
        };

        try {
            ctx.getConnection().transferPageCache(head, null, getMessageResult.getMessageBufferList(), 0,
                getMessageResult.getBufferTotalSize(), request.getOpaque(), listener, 10000L,
                TimeUnit.MILLISECONDS);
        }
        catch (NotifyRemotingException e1) {
            getMessageResult.release();
        }
    }


    private void writePageCacheToSlave(final GetCommand request, final SessionContext ctx,
            final SelectMapedBufferResult selectMapedBufferResult) {

        int size = selectMapedBufferResult.getByteBuffer().limit();
        final IoBuffer head = this.makeHead(request.getOpaque(), size);

        SingleRequestCallBackListener listener = new SingleRequestCallBackListener() {

            @Override
            public void onResponse(ResponseCommand responseCommand, Connection conn) {
                selectMapedBufferResult.release();

            }


            @Override
            public void onException(Exception e) {
                selectMapedBufferResult.release();
            }


            @Override
            public ThreadPoolExecutor getExecutor() {
                return null;
            }
        };

        try {
            final List<ByteBuffer> bufferList = new ArrayList<ByteBuffer>();
            bufferList.add(selectMapedBufferResult.getByteBuffer());
            ctx.getConnection().transferPageCache(head, null, bufferList, 0, size, request.getOpaque(), listener,
                10000L, TimeUnit.MILLISECONDS);
        }
        catch (NotifyRemotingException e1) {
            selectMapedBufferResult.release();
        }
    }


    /*
     * (non-Javadoc)
     * 
     * @see
     * com.taobao.metamorphosis.server.CommandProcessor#processGetCommand(com
     * .taobao.metamorphosis.network.GetCommand,
     * com.taobao.metamorphosis.server.network.SessionContext, boolean)
     */
    @Override
    public ResponseCommand processGetCommand(final GetCommand request, final SessionContext ctx,
            final boolean zeroCopy) {
        final String group = request.getGroup();
        final String topic = request.getTopic();
        final long reqOffset = request.getOffset();
        final int maxSize = request.getMaxSize();

        // 如果分区被关闭并且不是slave发起的请求,禁止读数据 --wuhua
        if (this.metaConfig.isClosedPartition(topic, request.getPartition())
                && !request.getGroup().equals(this.metaConfig.getSlaveGroup())) {
            log.warn("can not get message for topic=" + topic + " from partition " + request.getPartition()
                    + ",it closed,");
            return new BooleanCommand(request.getOpaque(), HttpStatus.Forbidden, "Partition["
                    + this.metaConfig.getBrokerId() + "-" + request.getPartition() + "] has been closed");
        }

        final MetaStore metaStore = this.getMetaStore();

        // 如果是slave来拉数据，则直接返回物理分区数据
        if (request.getGroup().equals(this.metaConfig.getSlaveGroup())) {
            SelectMapedBufferResult selectMapedBufferResult = metaStore.getPhyQueueData(reqOffset);
            if (selectMapedBufferResult != null && selectMapedBufferResult.getSize() > 0) {
                if (selectMapedBufferResult.getByteBuffer().limit() > maxSize) {
                    selectMapedBufferResult.getByteBuffer().limit(maxSize);
                }

                this.writePageCacheToSlave(request, ctx, selectMapedBufferResult);
                return null;
            }
            else {
                return new BooleanCommand(request.getOpaque(), HttpStatus.NotFound, "master no data");
            }
        }
        Set<Integer> messageTypeList = this.messageTypeManager.getMessageTypeHash(group, topic);
        GetMessageResult getMessageResult =
                metaStore.getMessage(topic, request.getPartition(), reqOffset, request.getMaxSize(), messageTypeList);
        if (getMessageResult != null) {
            switch (getMessageResult.getStatus()) {
            // 找到消息
            case FOUND: {
                if (zeroCopy) {
                    this.writePageCache(request, ctx, getMessageResult);
                    return null;
                }
                break;
            }
            // offset正确，但是过滤后没有匹配的消息
            case NO_MATCHED_MESSAGE: {
                return new BooleanCommand(request.getOpaque(), HttpStatus.Moved, String.valueOf(getMessageResult
                    .getNextBeginOffset()));
            }
            // offset正确，但是物理队列消息正在被删除
            case MESSAGE_WAS_REMOVING: {
                return new BooleanCommand(request.getOpaque(), HttpStatus.Moved, String.valueOf(getMessageResult
                    .getNextBeginOffset()));
            }
            // offset正确，但是从逻辑队列没有找到，可能正在被删除
            case OFFSET_FOUND_NULL: {
                return new BooleanCommand(request.getOpaque(), HttpStatus.Moved, String.valueOf(getMessageResult
                    .getNextBeginOffset()));
            }
            // offset错误，严重溢出
            case OFFSET_OVERFLOW_BADLY: {
                return new BooleanCommand(request.getOpaque(), HttpStatus.Moved, String.valueOf(getMessageResult
                    .getMaxOffset()));
            }
            // offset错误，溢出1个
            case OFFSET_OVERFLOW_ONE: {
                return new BooleanCommand(request.getOpaque(), HttpStatus.NotFound, getMessageResult.getStatus()
                    .toString());
            }
            // offset错误，太小了
            case OFFSET_TOO_SMALL: {
                return new BooleanCommand(request.getOpaque(), HttpStatus.Moved, String.valueOf(getMessageResult
                    .getMinOffset()));
            }
            // 没有对应的逻辑队列
            case NO_MATCHED_LOGIC_QUEUE: {
                return new BooleanCommand(request.getOpaque(), HttpStatus.NotFound, getMessageResult.getStatus()
                    .toString());
            }
            // 队列中一条消息都没有
            case NO_MESSAGE_IN_QUEUE: {
                return new BooleanCommand(request.getOpaque(), HttpStatus.NotFound, getMessageResult.getStatus()
                    .toString());
            }
            default: {
                break;
            }
            }
        }

        return new BooleanCommand(request.getOpaque(), HttpStatus.InternalServerError,
            "Metaq server service unavailable.");
    }


    @Override
    public ResponseCommand processOffsetCommand(final OffsetCommand request, final SessionContext ctx) {
        final MetaStore metaStore = this.getMetaStore();
        final long offset = metaStore.getMaxOffsetInQuque(request.getTopic(), request.getPartition());
        return new BooleanCommand(request.getOpaque(), HttpStatus.Success, String.valueOf(offset));
    }


    @Override
    public void processQuitCommand(final QuitCommand request, final SessionContext ctx) {
        try {
            if (ctx.getConnection() != null) {
                ctx.getConnection().close(false);
            }
        }
        catch (final NotifyRemotingException e) {
            // ignore
        }

    }


    @Override
    public ResponseCommand processVesionCommand(final VersionCommand request, final SessionContext ctx) {
        return new BooleanCommand(request.getOpaque(), HttpStatus.Success, BuildProperties.VERSION);

    }


    @Override
    public ResponseCommand processStatCommand(final StatsCommand request, final SessionContext ctx) {
        final String item = request.getItem();
        final String statsInfo = this.statsManager.getStatsInfo(item);
        return new BooleanCommand(request.getOpaque(), HttpStatus.Success, statsInfo);

    }


    @Override
    public ResponseCommand processAskCommand(AskCommand request, final SessionContext ctx) {
        String result = this.statsManager.getAskInfo(request.getTopic(), request.getType(), request.getParams());
        return new BooleanCommand(request.getOpaque(), HttpStatus.Success, result);
    }


    @Override
    public void removeTransaction(final XATransactionId xid) {
        throw new UnsupportedOperationException("Unsupported removeTransaction");
    }


    @Override
    public Transaction getTransaction(final SessionContext context, final TransactionId xid)
            throws MetamorphosisException, XAException {
        throw new UnsupportedOperationException("Unsupported getTransaction");
    }


    @Override
    public void forgetTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        throw new UnsupportedOperationException("Unsupported forgetTransaction");
    }


    @Override
    public void rollbackTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        throw new UnsupportedOperationException("Unsupported rollbackTransaction");
    }


    @Override
    public void commitTransaction(final SessionContext context, final TransactionId xid, final boolean onePhase)
            throws Exception {
        throw new UnsupportedOperationException("Unsupported commitTransaction");
    }


    @Override
    public int prepareTransaction(final SessionContext context, final TransactionId xid) throws Exception {
        throw new UnsupportedOperationException("Unsupported prepareTransaction");
    }


    @Override
    public void beginTransaction(final SessionContext context, final TransactionId xid, final int seconds)
            throws Exception {
        throw new UnsupportedOperationException("Unsupported beginTransaction");
    }


    @Override
    public TransactionId[] getPreparedTransactions(final SessionContext context) throws Exception {
        throw new UnsupportedOperationException("Unsupported getPreparedTransactions");
    }
    
    @Override
    public ResponseCommand processFetchCommand(FetchCommand request, SessionContext ctx) {
    	Set<String> messageTypes = this.messageTypeManager.getMessageType(request.getGroup(), request.getTopic(), 
    			request.getClientStartTime());
    	if(messageTypes == null){//如果没有客户端订阅的消息类型信息，需要让客户端发送一份
    		return new BooleanCommand(request.getOpaque(), HttpStatus.Continue, "messageType not found.");
    	}
    	return processGetCommand(request, ctx);
    }
    
	@Override
	public ResponseCommand processMessageTypeCommand(MessageTypeCommand request, SessionContext ctx) {
		String group = request.getGroup();
		Set<String> messageTypes = request.getMessageTypes();
		String topic = request.getTopic();
		long version = request.getClientStartTime();
		Set<String> messageTypeList = this.messageTypeManager.updateMessageType(group, topic, messageTypes, version);
		BooleanCommand bCmd = new BooleanCommand(request.getOpaque(), HttpStatus.Success, messageTypeList.toString());
		return bCmd;
	}
	
}

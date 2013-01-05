package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.PutCommand;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.server.CommandProcessor;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * Put请求处理器
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public class PutProcessor implements RequestProcessor<PutCommand> {
    static final Log log = LogFactory.getLog("PutLog");

    private final CommandProcessor processor;
    private final ThreadPoolExecutor executor;


    public PutProcessor(final CommandProcessor processor, final ThreadPoolExecutor executor) {
        super();
        this.processor = processor;
        this.executor = executor;
    }


    @Override
    public ThreadPoolExecutor getExecutor() {
        return this.executor;
    }


    @Override
    public void handleRequest(final PutCommand request, final Connection conn) {
        final TransactionId xid = request.getTransactionId();
        final SessionContext context = SessionContextHolder.getOrCreateSessionContext(conn, xid);
        try {
            this.processor.processPutCommand(request, context, new PutCallback() {
                @Override
                public void putComplete(final ResponseCommand resp) {
                    RemotingUtils.response(context.getConnection(), resp);
                    
                    // 记录日志
                    if (log.isDebugEnabled()) {
                        final BooleanCommand bc = (BooleanCommand)resp;
                        
                        final StringBuilder sb = new StringBuilder("");
                        // topic
                        sb.append("T:").append(request.getTopic()).append(" ");
                        // ResponseStatus
                        sb.append("R:").append(bc.getResponseStatus()).append(" ");
                        // result string
                        sb.append("S:").append(bc.getErrorMsg()).append(" ");
                        // client address
                        final String addrString = 
                            conn != null ? com.taobao.gecko.core.util.RemotingUtils.getAddrString(conn.getRemoteSocketAddress()) : "unknown";
                        sb.append("C:").append(addrString);
                        log.debug(sb.toString());
                    }
                }
            });
        }
        catch (final Exception e) {
            RemotingUtils.response(context.getConnection(), new BooleanCommand(request.getOpaque(),
                HttpStatus.InternalServerError, e.getMessage()));
            log.error(e.getMessage());
            
        }
    }

}

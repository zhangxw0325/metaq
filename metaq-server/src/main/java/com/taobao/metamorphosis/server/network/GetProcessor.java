package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.GetCommand;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.server.CommandProcessor;


/**
 * get«Î«Û¥¶¿Ì∆˜
 * 
 * @author boyan
 * @Date 2011-4-21
 * 
 */
public class GetProcessor implements RequestProcessor<GetCommand> {

    public static final Log log = LogFactory.getLog(GetProcessor.class);

    private final ThreadPoolExecutor executor;

    private final CommandProcessor processor;


    public GetProcessor(final CommandProcessor processor, final ThreadPoolExecutor executor) {
        super();
        this.processor = processor;
        this.executor = executor;
    }


    @Override
    public ThreadPoolExecutor getExecutor() {
        return this.executor;
    }


    @Override
    public void handleRequest(final GetCommand request, final Connection conn) {
        final ResponseCommand response =
                this.processor.processGetCommand(request, SessionContextHolder.getOrCreateSessionContext(conn, null));
        if (response != null) {
            RemotingUtils.response(conn, response);
        }
    }
}

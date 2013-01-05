package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.network.StatsCommand;
import com.taobao.metamorphosis.server.CommandProcessor;


/**
 * 统计信息查询处理器
 * 
 * @author boyan
 * @Date 2011-5-6
 * 
 */
public class StatsProcessor implements RequestProcessor<StatsCommand> {
    private final CommandProcessor processor;


    public StatsProcessor(final CommandProcessor processor) {
        super();
        this.processor = processor;
    }


    @Override
    public ThreadPoolExecutor getExecutor() {
        return null;
    }


    @Override
    public void handleRequest(final StatsCommand request, final Connection conn) {
        final ResponseCommand response =
                this.processor.processStatCommand(request, SessionContextHolder.getOrCreateSessionContext(conn, null));
        if (response != null) {
            RemotingUtils.response(conn, response);
        }
    }

}

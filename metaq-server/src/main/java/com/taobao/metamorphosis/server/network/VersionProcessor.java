package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.core.command.kernel.HeartBeatRequestCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.network.VersionCommand;
import com.taobao.metamorphosis.server.CommandProcessor;


/**
 * 处理版本查询请求，替代默认的心跳处理器
 * 
 * @author boyan
 * @Date 2011-4-22
 * 
 */
public class VersionProcessor implements RequestProcessor<HeartBeatRequestCommand> {

    private final CommandProcessor processor;


    public VersionProcessor(final CommandProcessor processor) {
        super();
        this.processor = processor;
    }


    @Override
    public void handleRequest(final HeartBeatRequestCommand request, final Connection conn) {
        final ResponseCommand response =
                this.processor.processVesionCommand((VersionCommand) request,
                    SessionContextHolder.getOrCreateSessionContext(conn, null));
        if (response != null) {
            RemotingUtils.response(conn, response);
        }
    }


    @Override
    public ThreadPoolExecutor getExecutor() {
        return null;
    }

}

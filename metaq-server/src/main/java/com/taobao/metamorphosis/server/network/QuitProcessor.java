package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.QuitCommand;
import com.taobao.metamorphosis.server.CommandProcessor;


/**
 * ÍË³öÃüÁî´¦ÀíÆ÷
 * 
 * @author boyan
 * @Date 2011-4-22
 * 
 */
public class QuitProcessor implements RequestProcessor<QuitCommand> {
    private final CommandProcessor processor;


    public QuitProcessor(final CommandProcessor processor) {
        super();
        this.processor = processor;
    }


    @Override
    public ThreadPoolExecutor getExecutor() {
        return null;
    }


    @Override
    public void handleRequest(final QuitCommand request, final Connection conn) {
        this.processor.processQuitCommand(request, SessionContextHolder.getOrCreateSessionContext(conn, null));
    }

}

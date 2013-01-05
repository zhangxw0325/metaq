package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.AskCommand;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.server.CommandProcessor;

public class AskProcessor implements RequestProcessor<AskCommand> {
	private final CommandProcessor processor;
	
	
	public AskProcessor(final CommandProcessor processor) {
        super();
        this.processor = processor;
    }
	
	
	@Override
    public ThreadPoolExecutor getExecutor() {
        return null;
    }


    @Override
    public void handleRequest(final AskCommand request, final Connection conn) {
        try {
			final ResponseCommand response =
		            this.processor.processAskCommand(request, SessionContextHolder.getOrCreateSessionContext(conn, null));
		    if (response != null) {
		        RemotingUtils.response(conn, response);
		    }
        }
        catch(Exception e) {
        	RemotingUtils.response(conn, new BooleanCommand(request.getOpaque(), 
        			HttpStatus.InternalServerError, e.getMessage()));
        }
    }
}

package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.MessageTypeCommand;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.server.CommandProcessor;

public class MessageTypeProcessor implements RequestProcessor<MessageTypeCommand> {

	private static final Log log = LogFactory.getLog(MessageTypeProcessor.class);

	private final ThreadPoolExecutor executor;

	private final CommandProcessor processor;

	public MessageTypeProcessor(final CommandProcessor processor, final ThreadPoolExecutor executor) {
		this.processor = processor;
		this.executor = executor;
	}

	@Override
	public void handleRequest(MessageTypeCommand request, Connection conn) {
		final SessionContext context = SessionContextHolder.getOrCreateSessionContext(conn, null);
		try {
			ResponseCommand resp = this.processor.processMessageTypeCommand(request, context);
			RemotingUtils.response(conn, resp);
		} catch (Exception e) {
			log.error(e.getMessage(), e.getCause());
			RemotingUtils.response(conn, new BooleanCommand(request.getOpaque(),
					HttpStatus.InternalServerError, e.getMessage()));
		}
	}

	@Override
	public ThreadPoolExecutor getExecutor() {
		return this.executor;
	}

}

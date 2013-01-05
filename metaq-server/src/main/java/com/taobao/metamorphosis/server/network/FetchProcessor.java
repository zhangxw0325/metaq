package com.taobao.metamorphosis.server.network;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.service.Connection;
import com.taobao.gecko.service.RequestProcessor;
import com.taobao.metamorphosis.network.BooleanCommand;
import com.taobao.metamorphosis.network.FetchCommand;
import com.taobao.metamorphosis.network.HttpStatus;
import com.taobao.metamorphosis.network.RemotingUtils;
import com.taobao.metamorphosis.server.CommandProcessor;

public class FetchProcessor implements RequestProcessor<FetchCommand> {

	static final Log log = LogFactory.getLog(FetchProcessor.class);

	private final ThreadPoolExecutor executor;

	private final CommandProcessor processor;

	public FetchProcessor(final CommandProcessor processor, final ThreadPoolExecutor executor) {
		this.processor = processor;
		this.executor = executor;
	}

	@Override
	public void handleRequest(FetchCommand request, Connection conn) {
		final SessionContext context = SessionContextHolder.getOrCreateSessionContext(conn, null);
		try {
			final ResponseCommand response = this.processor.processFetchCommand(request, context);
			if (response != null) {
				RemotingUtils.response(conn, response);
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e.getCause());
			RemotingUtils.response(context.getConnection(), new BooleanCommand(request.getOpaque(),
					HttpStatus.InternalServerError, e.getMessage()));
		}

	}

	@Override
	public ThreadPoolExecutor getExecutor() {
		return this.executor;
	}

}

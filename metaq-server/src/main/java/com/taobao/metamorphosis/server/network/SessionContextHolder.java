package com.taobao.metamorphosis.server.network;

import com.taobao.gecko.service.Connection;
import com.taobao.metamorphosis.transaction.LocalTransactionId;
import com.taobao.metamorphosis.transaction.TransactionId;


/**
 * SessionContext管理类
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-18
 * 
 */
public class SessionContextHolder {
    private SessionContextHolder() {

    }

    public static final String GLOBAL_SESSION_KEY = "SessionContextGlobalKey" + System.currentTimeMillis();


    public static SessionContext getOrCreateSessionContext(final Connection conn, final TransactionId xid) {
        SessionContext context = null;
        if (xid != null && xid.isLocalTransaction()) {
            // 本地事务带有session id，因此用sessionId做key存储
            final LocalTransactionId id = (LocalTransactionId) xid;
            context = (SessionContext) conn.getAttribute(id.getSessionId());
            if (context == null) {
                context = new SessionContextImpl(id.getSessionId(), conn);
                final SessionContext old = (SessionContext) conn.setAttributeIfAbsent(id.getSessionId(), context);
                if (old != null) {
                    context = old;
                }
            }
        }
        else {
            // XA事务没有session id，使用公共key，减少重复new
            context = (SessionContext) conn.getAttribute(GLOBAL_SESSION_KEY);
            if (context == null) {
                context = new SessionContextImpl(null, conn);
                final SessionContext old = (SessionContext) conn.setAttributeIfAbsent(GLOBAL_SESSION_KEY, context);
                if (old != null) {
                    context = old;
                }
            }

        }
        return context;
    }
}

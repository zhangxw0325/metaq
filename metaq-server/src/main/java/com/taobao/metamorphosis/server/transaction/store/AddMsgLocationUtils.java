package com.taobao.metamorphosis.server.transaction.store;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import com.taobao.metamorphosis.server.transaction.store.JournalTransactionStore.AddMsgLocation;


/**
 * 添加消息位置的序列化工具类
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-22
 * 
 */
public class AddMsgLocationUtils {

    public static ByteBuffer encodeLocation(final Map<String, JournalTransactionStore.AddMsgLocation> locations) {
        int capactity = 0;
        for (final Map.Entry<String, JournalTransactionStore.AddMsgLocation> entry : locations.entrySet()) {
            capactity += entry.getValue().encode().remaining();
        }
        final ByteBuffer buf = ByteBuffer.allocate(capactity);
        for (final Map.Entry<String, JournalTransactionStore.AddMsgLocation> entry : locations.entrySet()) {
            buf.put(entry.getValue().encode());
        }
        buf.flip();
        return buf;
    }


    public static final Map<String, JournalTransactionStore.AddMsgLocation> decodeLocations(final ByteBuffer buf) {

        AddMsgLocation location = null;

        final Map<String, JournalTransactionStore.AddMsgLocation> rt =
                new LinkedHashMap<String, JournalTransactionStore.AddMsgLocation>();

        while ((location = AddMsgLocation.decode(buf)) != null) {
            rt.put(location.storeDesc, location);
        }
        return rt;
    }
}

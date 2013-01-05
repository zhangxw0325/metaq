package com.taobao.metamorphosis.server.utils;

import java.util.Random;

import com.taobao.metamorphosis.transaction.XATransactionId;


/**
 * 产生xid的工具类，仅用于测试
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-24
 * 
 */
public class XIDGenerator {
    private final static Random rand = new Random();


    private static byte[] randomBytes() {
        final byte[] bytes = new byte[rand.nextInt(100)];
        rand.nextBytes(bytes);
        return bytes;
    }


    public static XATransactionId createXID(final int formatId) {
        final byte[] branchQualifier = randomBytes();
        final byte[] globalTransactionId = randomBytes();
        final XATransactionId xid = new XATransactionId(formatId, branchQualifier, globalTransactionId);
        return xid;
    }
}

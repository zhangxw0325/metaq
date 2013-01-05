package com.taobao.metamorphosis.transaction;

import java.io.Serializable;


/**
 * 事务id包装类
 * 
 * @author boyan
 * 
 */
public abstract class TransactionId implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 2157471469471230263L;
    public static final NullTransactionId Null = new NullTransactionId();


    public abstract boolean isXATransaction();


    public abstract boolean isLocalTransaction();


    public abstract String getTransactionKey();


    public abstract boolean isNull();


    public static TransactionId valueOf(final String key) {
        if (key.equals("null")) {
            return TransactionId.Null;
        }
        else if (key.startsWith("XID:")) {
            return new XATransactionId(key);
        }
        else if (key.startsWith("TX:")) {
            return new LocalTransactionId(key);

        }
        else {
            throw new IllegalArgumentException("Illegal transaction key:" + key);
        }
    }

}

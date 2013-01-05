package com.taobao.metamorphosis.client.extension.producer;

import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 表示某topic当前可用的分区个数不正确,比如跟期望的总数不一致等
 * 
 * @author 无花
 * @since 2011-8-2 下午02:49:27
 */

public class AvailablePartitionNumException extends MetaClientException {

    private static final long serialVersionUID = 8087499474643513774L;


    public AvailablePartitionNumException() {
        super();
    }


    public AvailablePartitionNumException(String message, Throwable cause) {
        super(message, cause);
    }


    public AvailablePartitionNumException(String message) {
        super(message);
    }


    public AvailablePartitionNumException(Throwable cause) {
        super(cause);
    }

}

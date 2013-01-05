package com.taobao.metamorphosis.client;

import com.taobao.metamorphosis.exception.MetaClientException;


/**
 * 可关闭服务接口
 * 
 * @author boyan
 * @Date 2011-6-2
 * 
 */
public interface Shutdownable {
    public void shutdown() throws MetaClientException;
}

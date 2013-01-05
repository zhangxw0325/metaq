package com.taobao.metamorphosis.gregor.slave;



/**
 * 
 * 分类有序任务
 * 
 * @see OrderedThreadPoolExecutor
 */
public interface IoEvent extends Runnable {

    public IoCatalog getIoCatalog();
}

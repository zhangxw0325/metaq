package com.taobao.metamorphosis.server.assembly;

/**
 * 事务处理器MBean接口，提供一些查询和管理的API
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-29
 * 
 */
public interface TransactionalCommandProcessorMBean {

    /**
     * 返回所有处于prepare状态的xa事务
     * 
     * @return
     */
    public String[] getPreparedTransactions() throws Exception;


    /**
     * 返回所有处于prepare状态的xa事务数目
     * 
     * @return
     */
    public int getPreparedTransactionCount() throws Exception;


    /**
     * 人工提交事务
     * 
     * @param txKey
     */
    public void commitTransactionHeuristically(String txKey, boolean onePhase) throws Exception;


    /**
     * 人工回滚事务
     * 
     * @param txKey
     */
    public void rollbackTransactionHeuristically(String txKey) throws Exception;


    /**
     * 人工完成事务，不提交也不回滚，简单删除
     * 
     * @param txKey
     * @throws Exception
     */
    public void completeTransactionHeuristically(String txKey) throws Exception;

}

package com.taobao.metamorphosis.utils.test;

/**
 * 
 * 
 * 并发测试任务接口
 * 
 * @author boyan
 * 
 * @since 1.0, 2010-1-11 下午03:11:58
 */

public interface ConcurrentTestTask {
    /**
     * 
     * @param index
     *            线程索引号
     * @param times
     *            次数
     * @throws Exception TODO
     */
    public void run(int index, int times) throws Exception;
}

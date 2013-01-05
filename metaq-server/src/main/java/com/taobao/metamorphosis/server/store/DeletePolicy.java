package com.taobao.metamorphosis.server.store;

import java.io.File;


/**
 * 文件的删除策略
 * 
 * @author boyan
 * @Date 2011-4-29
 * 
 */
public interface DeletePolicy {
    /**
     * 判断文件是否可以删除
     * 
     * @param file
     * @param checkTimestamp
     * @return
     */
    public boolean canDelete(File file, long checkTimestamp);


    /**
     * 处理过期文件
     * 
     * @param file
     */
    public void process(File file);


    /**
     * 策略名称
     * 
     * @return
     */
    public String name();


    /**
     * 初始化
     * 
     * @param values
     */
    public void init(String... values);
}

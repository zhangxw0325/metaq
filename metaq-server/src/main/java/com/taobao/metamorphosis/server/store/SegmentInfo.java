package com.taobao.metamorphosis.server.store;

/**
 * 分区内各个文件的信息
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-15
 * 
 */
public class SegmentInfo {
    public final long startOffset;
    public final long size;


    public SegmentInfo(final long startOffset, final long size) {
        super();
        this.startOffset = startOffset;
        this.size = size;
    }

}

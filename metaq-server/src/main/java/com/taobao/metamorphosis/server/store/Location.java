package com.taobao.metamorphosis.server.store;

/**
 * 数据存入的位置
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-22
 * 
 */
public class Location {
    protected final long offset;
    protected final int length;

    public static Location InvalidLocaltion = new Location(-1, -1);


    public Location(final long offset, final int length) {
        super();
        this.offset = offset;
        this.length = length;
    }


    public long getOffset() {
        return this.offset;
    }


    public int getLength() {
        return this.length;
    }

}

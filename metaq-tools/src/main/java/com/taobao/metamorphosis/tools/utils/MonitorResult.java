package com.taobao.metamorphosis.tools.utils;

import java.util.Date;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-9-28 ÏÂÎç1:46:26
 */

public class MonitorResult {
    public String getKey() {
        return this.key;
    }


    public void setKey(String key) {
        this.key = key;
    }


    public String getDescribe() {
        return this.describe;
    }


    public void setDescribe(String describe) {
        this.describe = describe;
    }


    public Double getValue() {
        return this.value;
    }


    public void setValue(Double value) {
        this.value = value;
    }


    public Date getTime() {
        return this.time;
    }


    public void setTime(Date time) {
        this.time = time;
    }


    public String getIp() {
        return this.ip;
    }


    public void setIp(String ip) {
        this.ip = ip;
    }


    public int getId() {
        return this.id;
    }


    public void setId(int id) {
        this.id = id;
    }

    private int id;

    private String key;

    private String ip;

    private String describe;

    private Double value;

    private Date time;


    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}

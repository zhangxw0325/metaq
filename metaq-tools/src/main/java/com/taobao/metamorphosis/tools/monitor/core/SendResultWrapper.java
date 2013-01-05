package com.taobao.metamorphosis.tools.monitor.core;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.producer.SendResult;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-5-25 ÉÏÎç11:57:30
 */

public class SendResultWrapper {

    private Message message;

    private SendResult sendResult;

    private Exception e;


    public SendResultWrapper setMessage(Message message) {
        this.message = message;
        return this;
    }


    public Message getMessage() {
        return this.message;
    }


    public SendResult getSendResult() {
        return this.sendResult;
    }


    public void setSendResult(SendResult sendResult) {
        this.sendResult = sendResult;
    }


    public boolean isSuccess() {
        return this.sendResult != null && this.sendResult.isSuccess();
    }


    public Exception getException() {
        return this.e;
    }


    public void setException(Exception e) {
        this.e = e;
    }


    public String getErrorMessage() {
        return this.sendResult != null ? this.sendResult.getErrorMessage() : "sendResult is null";
    }
}

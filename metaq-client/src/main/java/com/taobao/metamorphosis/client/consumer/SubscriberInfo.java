package com.taobao.metamorphosis.client.consumer;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * ∂©‘ƒ’ﬂ–≈œ¢
 * 
 * @author boyan
 * @Date 2011-4-26
 * 
 */
public class SubscriberInfo {
    private final MessageListener messageListener;
    private final int maxSize;
    private final Set<String> messageTypes;

   


	public SubscriberInfo(final MessageListener messageListener, final int maxSize, final String[] messageTypes) {
        super();
        this.messageListener = messageListener;
        this.maxSize = maxSize;
        if(messageTypes != null){
        	this.messageTypes = new HashSet<String>();
        	for(String type : messageTypes){
        		if(!StringUtils.isBlank(type)){
        			this.messageTypes.add(type);
        		}
        	}
        } else {
        	this.messageTypes = null;
        }
    } 


    public MessageListener getMessageListener() {
        return this.messageListener;
    }
    
    


    public Set<String> getMessageTypes() {
		return messageTypes;
	}


	@Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.maxSize;
        result = prime * result + (this.messageListener == null ? 0 : this.messageListener.hashCode());
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final SubscriberInfo other = (SubscriberInfo) obj;
        if (this.maxSize != other.maxSize) {
            return false;
        }
        if (this.messageListener == null) {
            if (other.messageListener != null) {
                return false;
            }
        }
        else if (!this.messageListener.equals(other.messageListener)) {
            return false;
        }
        return true;
    }


    public int getMaxSize() {
        return this.maxSize;
    }

}

package com.taobao.metamorphosis.network;

import java.net.InetSocketAddress;

import com.taobao.gecko.core.command.ResponseCommand;
import com.taobao.gecko.core.command.ResponseStatus;


/**
 * Ó¦´ðÃüÁî»ùÀà
 * 
 * @author boyan
 * @Date 2011-6-2
 * 
 */
public abstract class AbstractResponseCommand implements ResponseCommand, MetaEncodeCommand {
    private Integer opaque;
    private InetSocketAddress responseHost;
    private long responseTime;
    private ResponseStatus responseStatus;
    static final long serialVersionUID = -1L;


    public AbstractResponseCommand(final Integer opaque) {
        super();
        this.opaque = opaque;
    }


    @Override
    public Integer getOpaque() {
        return this.opaque;
    }


    @Override
    public InetSocketAddress getResponseHost() {
        return this.responseHost;
    }


    @Override
    public void setResponseHost(final InetSocketAddress responseHost) {
        this.responseHost = responseHost;
    }


    @Override
    public long getResponseTime() {
        return this.responseTime;
    }


    @Override
    public void setResponseTime(final long responseTime) {
        this.responseTime = responseTime;
    }


    @Override
    public ResponseStatus getResponseStatus() {
        return this.responseStatus;
    }


    @Override
    public void setResponseStatus(final ResponseStatus responseStatus) {
        this.responseStatus = responseStatus;
    }


    @Override
    public void setOpaque(final Integer opaque) {
        this.opaque = opaque;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.opaque == null ? 0 : this.opaque.hashCode());
        result = prime * result + (this.responseHost == null ? 0 : this.responseHost.hashCode());
        result = prime * result + (this.responseStatus == null ? 0 : this.responseStatus.hashCode());
        result = prime * result + (int) (this.responseTime ^ this.responseTime >>> 32);
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
        final AbstractResponseCommand other = (AbstractResponseCommand) obj;
        if (this.opaque == null) {
            if (other.opaque != null) {
                return false;
            }
        }
        else if (!this.opaque.equals(other.opaque)) {
            return false;
        }
        if (this.responseHost == null) {
            if (other.responseHost != null) {
                return false;
            }
        }
        else if (!this.responseHost.equals(other.responseHost)) {
            return false;
        }
        if (this.responseStatus == null) {
            if (other.responseStatus != null) {
                return false;
            }
        }
        else if (!this.responseStatus.equals(other.responseStatus)) {
            return false;
        }
        if (this.responseTime != other.responseTime) {
            return false;
        }
        return true;
    }

}

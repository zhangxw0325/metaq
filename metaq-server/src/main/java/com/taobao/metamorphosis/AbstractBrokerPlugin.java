package com.taobao.metamorphosis;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;


/**
 * @author ÎÞ»¨
 * @since 2011-6-9 ÏÂÎç02:22:38
 */

abstract public class AbstractBrokerPlugin implements BrokerPlugin {

    protected static final Log log = LogFactory.getLog(AbstractBrokerPlugin.class);

    protected MetaMorphosisBroker broker;
    protected Properties props;


    @Override
    public boolean equals(final Object obj) {

        if (this == obj) {
            return true;
        }
        if (obj instanceof BrokerPlugin) {
            final BrokerPlugin that = (BrokerPlugin) obj;
            if (this.name() == that.name()) {
                return true;
            }
            if (this.name() != null) {
                return this.name().equals(that.name());
            }
            if (this.name() == null) {
                return that.name() == null;
            }
        }
        return false;
    }


    @Override
    public int hashCode() {
        return this.name() != null ? this.name().hashCode() : 0;
    }
}

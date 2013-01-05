package com.taobao.metamorphosis.utils;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;


/**
 * ע�ᵽƽ̨Mbean server
 * 
 * @author boyan
 * @Date 2011-7-14
 * 
 */
public class MetaMBeanServer {
    public static void registMBean(Object o, String name) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        if (null != mbs) {
            try {
                mbs.registerMBean(o, new ObjectName(o.getClass().getPackage().getName() + ":type="
                        + o.getClass().getSimpleName()
                        + (null == name ? ",id=" + o.hashCode() : ",name=" + name + "-" + o.hashCode())));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}

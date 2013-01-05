package com.taobao.metamorphosis.tools.monitor.alert;

import java.util.Arrays;
import java.util.List;

import com.taobao.metamorphosis.tools.monitor.core.MonitorConfig;
import com.taobao.notify.msgcenter.PushMsg;


/**
 * @author ÎÞ»¨
 * @since 2011-5-25 ÏÂÎç04:07:28
 */

public class Alarm {
	
	public static boolean needAlert = true;

    static {
        PushMsg.setWangwangTitle("metamorphosis-monitor-alert");
    }


    public static void alert(String msg, MonitorConfig monitorConfig) {
    
    	if(needAlert){
    		start().wangwangs(monitorConfig.getWangwangList()).mobiles(monitorConfig.getMobileList()).alert(msg);
    	}
    }


    public static EachAlarm start() {
        return new EachAlarm();
    }

    public static class EachAlarm {
        private List<String> wangwangList;
        private List<String> mobileList;
        private boolean isNeedMobileAlert = true;


        public EachAlarm wangwangs(List<String> wangwangList) {
            this.wangwangList = wangwangList;
            return this;
        }


        public EachAlarm wangwangs(String... wangwangs) {
            if (wangwangs != null && wangwangs.length > 0) {
                this.wangwangs(Arrays.asList(wangwangs));
            }
            return this;
        }


        public EachAlarm mobiles(List<String> mobileList) {
            this.mobileList = mobileList;
            return this;
        }


        public EachAlarm mobileAlert(boolean isNeedMobileAlert) {
            this.isNeedMobileAlert = isNeedMobileAlert;
            return this;
        }


        public void alert(String msg) {
        	
        	if(needAlert){
	            if (this.wangwangList != null && !this.wangwangList.isEmpty()) {
	                for (String wangwang : this.wangwangList) {
	                    PushMsg.alertByWW(wangwang, msg);
	                }
	            }
	
	            if (this.mobileList != null && !this.mobileList.isEmpty() && this.isNeedMobileAlert) {
	                for (String mobile : this.mobileList) {
	                    PushMsg.alertByMobile(mobile, msg);
	                }
	            }
        	}
        }
    }

}

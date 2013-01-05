package com.taobao.metamorphosis.server.store;

import java.util.Collection;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;


/**
 * É¾³ýÈÎÎñµÄquartz job
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-12-23
 * 
 */
public class DeleteJob implements Job {
    public static final String TOPICS = "topics";
    public static final String STORE_MGR = "messageStoreManager";

    private static final Log log = LogFactory.getLog(DeleteJob.class);


    @Override
    public void execute(final JobExecutionContext context) throws JobExecutionException {
        final JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        final Set<String> topics = (Set<String>) dataMap.get(TOPICS);
        final MessageStoreManager storeManager = (MessageStoreManager) dataMap.get(STORE_MGR);
        for (final String topic : topics) {
            final Collection<MessageStore> msgStores = storeManager.getMessageStoresByTopic(topic);
            if (msgStores != null) {
                for (final MessageStore msgStore : msgStores) {
                    try {
                        msgStore.runDeletePolicy();
                    }
                    catch (final Throwable e) {
                        log.error("Try to run delete policy with " + msgStore.getDescription() + "  failed", e);
                    }
                }
            }
        }
    }

}

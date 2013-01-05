package com.taobao.metamorphosis.server.assembly;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.taobao.metamorphosis.server.Service;
import com.taobao.metamorphosis.server.utils.MetaConfig;
import com.taobao.metamorphosis.utils.NamedThreadFactory;


public class ExecutorsManager implements Service {
    ThreadPoolExecutor getExecutor;
    ThreadPoolExecutor unOrderedPutExecutor;


    public ExecutorsManager(final MetaConfig metaConfig) {
        super();
        this.getExecutor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(metaConfig.getGetProcessThreadCount(),
                    new NamedThreadFactory("GetProcess"));
        this.unOrderedPutExecutor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(metaConfig.getPutProcessThreadCount(),
                    new NamedThreadFactory("PutProcess"));

    }


    public ThreadPoolExecutor getGetExecutor() {
        return this.getExecutor;
    }


    public ThreadPoolExecutor getUnOrderedPutExecutor() {
        return this.unOrderedPutExecutor;
    }


    @Override
    public void dispose() {
        if (this.getExecutor != null) {
            this.getExecutor.shutdown();
        }

        if (this.unOrderedPutExecutor != null) {
            this.unOrderedPutExecutor.shutdown();
        }
    }


    @Override
    public void init() {
    }

}

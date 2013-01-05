package com.taobao.metamorphosis.tail4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.tail4j.Config.LogConfig;


public class Tail4j {
    static final int PORT = 8609;


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Useage: java com.taobao.metamorphosis.tail4j.Tail4j tail4j.ini");
            System.exit(1);
        }
        final ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(false);
        try {
            ss.bind(new InetSocketAddress(PORT));
        }
        catch (IOException e) {
            throw new RuntimeException("Tail4j has been startup");
        }

        String configPath = args[0];
        Config config = new Config();
        config.parseFromIni(configPath);

        List<LogConfig> logConfigs = config.getLogConfigs();
        if (logConfigs.isEmpty()) {
            throw new RuntimeException("Emtpy log configs");
        }

        final MetaClientConfig metaClientConfig = new MetaClientConfig();
        metaClientConfig.setServerUrl(config.getServerUrl());
        metaClientConfig.setDiamondZKDataId(config.getDiamondZkDataId());
        metaClientConfig.setDiamondZKGroup(config.getDiamondZkGroup());
        final MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);

        final List<Tail> threads = new ArrayList<Tail>();
        for (LogConfig logConfig : logConfigs) {
            Tail thread = new Tail(config, logConfig, sessionFactory);
            threads.add(thread);
            thread.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (Tail tail : threads) {
                    tail.shutdown();
                }
                for (Tail tail : threads) {
                    try {
                        tail.join(10000);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                try {
                    sessionFactory.shutdown();
                }
                catch (Exception e) {

                }
                try {
                    ss.close();
                }
                catch (IOException e) {

                }
            }
        });

    }
}

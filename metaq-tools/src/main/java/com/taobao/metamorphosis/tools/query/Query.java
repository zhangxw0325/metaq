package com.taobao.metamorphosis.tools.query;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.diamond.manager.DiamondManager;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;
import com.taobao.metamorphosis.cluster.Partition;
import com.taobao.metamorphosis.tools.monitor.InitException;
import com.taobao.metamorphosis.tools.query.OffsetQueryDO.QueryType;
import com.taobao.metamorphosis.tools.utils.StringUtil;
import com.taobao.metamorphosis.utils.DiamondUtils;
import com.taobao.metamorphosis.utils.MetaZookeeper;
import com.taobao.metamorphosis.utils.ResourceUtils;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.StringSerializer;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;


/**
 * offset查询参数
 * 
 * @author pingwei
 */
public class Query {
    ZkClient zkClient = null;
    Connection connect = null;

    ZkOffsetStorageQuery zkQuery = null;
    MysqlOffsetStorageQuery mysqlQuery = null;

    DiamondManager diamondManager;
    ZKConfig zkConfig;
    MetaZookeeper metaZookeeper;
    static final Log log = LogFactory.getLog(Query.class);


    public Query() {
    }


    public String queryOffset(OffsetQueryDO queryDO) {
        OffsetStorageQuery query = this.chooseQuery(queryDO.getType());
        return query.getOffset(queryDO);
    }


    private OffsetStorageQuery chooseQuery(QueryType type) {
        OffsetStorageQuery query = null;
        if (type == QueryType.zk) {
            if (this.zkClient == null) {
                System.out.println("there is no zkClient connect for query offset.");
            }
            query = this.zkQuery;
        }
        else if (type == QueryType.mysql) {
            if (this.connect == null) {
                System.out.println("there is no mysql connect for query offset");
            }
            query = this.mysqlQuery;
        }
        return query;
    }


    public void init(String serverConf, String jdbcConf) throws InitException {
        if (!StringUtil.empty(serverConf)) {
            try {
                this.zkConfig = this.initZkConfig(serverConf);
                this.zkClient = this.newZkClient(this.zkConfig);
                this.metaZookeeper = new MetaZookeeper(zkClient, zkConfig.zkRoot);
                this.zkQuery = new ZkOffsetStorageQuery(this.zkClient, metaZookeeper);
            }
            catch (IOException e) {
                throw new InitException("初始化zk客户端失败", e);
            }
        }
        if (!StringUtil.empty(jdbcConf)) {
            this.initMysqlClient(jdbcConf);
            this.mysqlQuery = new MysqlOffsetStorageQuery(this.connect);
        }

    }


    private void initMysqlClient(String jdbcConf) throws InitException {
        try {
            Properties jdbcProperties = ResourceUtils.getResourceAsProperties(jdbcConf);

            String url = jdbcProperties.getProperty("jdbc.url");
            String userName = jdbcProperties.getProperty("jdbc.username");
            String userPassword = jdbcProperties.getProperty("jdbc.password");
            String jdbcUrl = url + "&user=" + userName + "&password=" + userPassword;
            System.out.println("mysql connect parameter is :\njdbc.url=" + jdbcUrl);
            Class.forName("com.mysql.jdbc.Driver");
            this.connect = DriverManager.getConnection(jdbcUrl);
        }
        catch (FileNotFoundException e) {
            throw new InitException(e.getMessage(), e.getCause());
        }
        catch (Exception e) {
            throw new InitException("mysql connect init failed. " + e.getMessage(), e.getCause());
        }
    }


    private ZkClient newZkClient(ZKConfig zkConfig) throws InitException {
        return new ZkClient(zkConfig.zkConnect, zkConfig.zkSessionTimeoutMs, zkConfig.zkConnectionTimeoutMs,
            new StringSerializer());
    }


    private ZKConfig initZkConfig(String serverConf) throws IOException {
        Properties serverProperties = com.taobao.metamorphosis.utils.Utils.getResourceAsProperties(serverConf, "GBK");

        String zkConnect = serverProperties.getProperty("zk.zkConnect");
        final String zkRoot = serverProperties.getProperty("zk.zkConnect");
        if (!StringUtil.empty(zkConnect)) {
            int zkSessionTimeoutMs = Integer.parseInt(serverProperties.getProperty("zk.zkSessionTimeoutMs"));
            int zkConnectionTimeoutMs = Integer.parseInt(serverProperties.getProperty("zk.zkConnectionTimeoutMs"));
            int zkSyncTimeMs = Integer.parseInt(serverProperties.getProperty("zk.zkSyncTimeMs"));
            return this.setZkRoot(new ZKConfig(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs, zkSyncTimeMs),
                zkRoot);
        }
        else {
            String diamondZKDataId = serverProperties.getProperty("diamondZKDataId");
            String diamondZKGroup = serverProperties.getProperty("diamondZKGroup");

            synchronized (this) {
                if (this.diamondManager == null) {
                    this.diamondManager =
                            new DefaultDiamondManager(diamondZKGroup, diamondZKDataId, new ManagerListener() {
                                public void receiveConfigInfo(String configInfo) {
                                    log.info("receive diamond data : " + configInfo);
                                    final Properties properties = new Properties();
                                    try {
                                        properties.load(new StringReader(configInfo));
                                        Query.this.zkConfig =
                                                Query.this.setZkRoot(DiamondUtils.getZkConfig(properties), zkRoot);
                                        Query.this.zkClient.close();
                                        Thread.sleep(Query.this.zkConfig.zkSyncTimeMs);
                                        log.info("Initialize zk client...");
                                        Query.this.zkClient =
                                                new ZkClient(Query.this.zkConfig.zkConnect,
                                                    Query.this.zkConfig.zkSessionTimeoutMs,
                                                    Query.this.zkConfig.zkConnectionTimeoutMs,
                                                    new ZkUtils.StringSerializer());
                                    }
                                    catch (Exception e) {
                                        log.error("从diamond加载zk配置失败", e);
                                    }
                                }


                                public Executor getExecutor() {
                                    return null;
                                }
                            });
                }
            }
            return this.setZkRoot(DiamondUtils.getZkConfig(this.diamondManager, 10000), zkRoot);
        }
        // System.out.println("zkClient parameters is :\nzkConnect = " +
        // zkConnect + "\nzkSessionTimeoutMs = "
        // + zkSessionTimeoutMs + "\nzkConnectionTimeoutMs = " +
        // zkConnectionTimeoutMs);

    }


    private ZKConfig setZkRoot(ZKConfig zkConfig2, String zkRoot) {
        if (StringUtils.isNotBlank(zkRoot)) {
            zkConfig2.zkRoot = zkRoot;
        }
        return zkConfig2;
    }


    public ZKConfig getZkConfig(String serverConf) throws IOException {
        return this.zkConfig != null ? this.zkConfig : this.initZkConfig(serverConf);
    }


    // --------add by wuhua below---------
    public List<String/* group */> getConsumerGroups(QueryType type) {
        return this.chooseQuery(type).getConsumerGroups();
    }


    public List<String> getTopicsExistOffset(String group, QueryType type) {
        return this.chooseQuery(type).getTopicsExistOffset(group);
    }


    public List<String> getPartitionsOf(String group, String topic, QueryType type) {
        return this.chooseQuery(type).getPartitionsOf(group, topic);
    }


    public String getOffsetPath(String group, String topic, Partition partition) {
        return metaZookeeper.new ZKGroupTopicDirs(topic, group).consumerOffsetDir + "/" + partition.toString();
    }


    public ZkClient getZkClient() {
        return this.zkClient;
    }


    public void close() {
        if (this.zkClient != null) {
            this.zkClient.close();
        }
        if (this.diamondManager != null) {
            this.diamondManager.close();
            this.diamondManager = null;
        }
    }
}

package com.taobao.metamorphosis.client.consumer.storage;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.After;
import org.junit.Before;

import com.taobao.gecko.core.util.ResourcesUtils;


public class MysqlOffsetStorageUnitTest extends BaseOffsetStorageUnitTest {
    private BasicDataSource ds;


    @Before
    public void setUp() throws Exception {
        final Properties props = new Properties();
        final InputStream in = ResourcesUtils.getResourceAsStream("jdbc.properties");
        props.load(in);
        in.close();
        this.ds = new BasicDataSource();
        this.ds.setDriverClassName("com.mysql.jdbc.Driver");
        this.ds.setUrl(props.getProperty("jdbc.url"));
        this.ds.setUsername(props.getProperty("jdbc.username"));
        this.ds.setPassword(props.getProperty("jdbc.password"));
        this.ds.setInitialSize(Integer.parseInt(props.getProperty("jdbc.initialSize")));
        this.ds.setMaxActive(Integer.parseInt(props.getProperty("jdbc.maxActive")));
        this.ds.setMaxIdle(Integer.parseInt(props.getProperty("jdbc.maxIdle")));
        this.ds.setMaxWait(Long.parseLong(props.getProperty("jdbc.maxWait")));
        this.ds.setPoolPreparedStatements(Boolean.valueOf(props.getProperty("jdbc.poolPreparedStatements")));
        this.truncateTable();
        this.offsetStorage = new MysqlOffsetStorage(this.ds);
    }


    private void truncateTable() throws SQLException {
        final Connection connection = this.ds.getConnection();
        final Statement stmt = connection.createStatement();
        stmt.execute("truncate table " + MysqlOffsetStorage.DEFAULT_TABLE_NAME);
        stmt.close();
        connection.close();
    }


    @After
    public void tearDown() throws Exception {
        this.truncateTable();
        this.ds.close();
    }
}

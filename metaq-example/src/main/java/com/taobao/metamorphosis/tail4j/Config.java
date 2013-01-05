package com.taobao.metamorphosis.tail4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ini4j.Ini;

import com.taobao.diamond.common.Constants;
import com.taobao.gecko.core.util.ResourcesUtils;
import com.taobao.metamorphosis.utils.DiamondUtils;


/**
 * tail4j
 * 
 * @author boyan
 * @Date 2011-5-17
 * 
 */
public class Config {
    static final Log log = LogFactory.getLog(Config.class);
    private String checkPointPath;
    private int maxBufSize;

    private String serverUrl;
    private String diamondZkDataId;
    private String diamondZkGroup;

    private String testPath;

    private final List<LogConfig> logConfigs = new ArrayList<LogConfig>();


    public List<LogConfig> getLogConfigs() {
        return this.logConfigs;
    }


    public void parseFromIni(final String path) throws IOException {
        final File file = new File(path);
        if (!file.exists()) {
            throw new FileNotFoundException(path);
        }
        final Ini ini = new Ini();
        ini.load(file);

        final Map<String, String> systemConfig = ini.get("system");
        this.checkPointPath =
                this.get(systemConfig, "checkpoint_path", System.getProperty("user.home") + File.separator + "meta"
                        + File.separator + "tail4j");
        this.maxBufSize = Integer.parseInt(this.get(systemConfig, "max_buf_size", "16*1024"));

        final Map<String, String> metaConfig = ini.get("meta");
        this.serverUrl = this.get(metaConfig, "server_url", null);
        this.diamondZkDataId = this.get(metaConfig, "diamond_zk_data_id", DiamondUtils.DEFAULT_ZK_DATAID);// DiamondUtils.zkDataId
        this.diamondZkGroup = this.get(metaConfig, "diamond_zk_group", Constants.DEFAULT_GROUP);

        final Map<String, String> testConfig = ini.get("local_test");
        this.testPath = this.get(testConfig, "test_path", null);

        final Set<String> keySet = ini.keySet();
        for (final String key : keySet) {
            if (key.startsWith("topic_")) {
                final Map<String, String> subConfig = ini.get(key);
                final String topic = this.get(subConfig, "topic", key);
                final String tmpLogFullPath = this.get(subConfig, "tmp_log_fullpath", null);
                final String logBasePath = this.get(subConfig, "log_base_path", null);
                final String logNamePattern = this.get(subConfig, "log_name_regx", null);
                final String encoding = this.get(subConfig, "encoding", null);
                final boolean compress = Boolean.valueOf(this.get(subConfig, "compress", "false"));
                final boolean ordered = Boolean.valueOf(this.get(subConfig, "ordered", "true"));
                final long timeout = Long.parseLong(this.get(subConfig, "timeout", "10000"));
                final String checkPointName = this.get(subConfig, "checkpoint_name", key);
                this.logConfigs.add(new LogConfig(topic, logBasePath, logNamePattern, tmpLogFullPath, encoding,
                    compress, ordered, timeout, checkPointName));
            }
        }
        log.info("Load config successfully:" + this.toString());

    }


    private String get(final Map<String, String> section, final String key, final String defaultValue) {
        if (section == null) {
            return defaultValue;
        }
        if (!StringUtils.isBlank(section.get(key))) {
            return section.get(key);
        }
        else {
            return defaultValue;
        }
    }


    public String getCheckPointPath() {
        return this.checkPointPath;
    }


    public void setCheckPointPath(final String checkPointPath) {
        this.checkPointPath = checkPointPath;
    }


    public int getMaxBufSize() {
        return this.maxBufSize;
    }


    public void setMaxBufSize(final int maxBufSize) {
        this.maxBufSize = maxBufSize;
    }


    public String getServerUrl() {
        return this.serverUrl;
    }


    public void setServerUrl(final String serverUrl) {
        this.serverUrl = serverUrl;
    }


    public String getDiamondZkDataId() {
        return this.diamondZkDataId;
    }


    public void setDiamondZkDataId(final String diamondZkDataId) {
        this.diamondZkDataId = diamondZkDataId;
    }


    public String getDiamondZkGroup() {
        return this.diamondZkGroup;
    }


    public void setDiamondZkGroup(final String diamondZkGroup) {
        this.diamondZkGroup = diamondZkGroup;
    }


    public String getTestPath() {
        return this.testPath;
    }


    public void setTestPath(final String testPath) {
        this.testPath = testPath;
    }


    @Override
    public String toString() {
        return "Config [checkPointPath=" + this.checkPointPath + ", diamondZkDataId=" + this.diamondZkDataId
                + ", diamondZkGroup=" + this.diamondZkGroup + ", logConfigs=" + this.logConfigs + ", maxBufSize="
                + this.maxBufSize + ", serverUrl=" + this.serverUrl + ", testPath=" + this.testPath + "]";
    }

    public static class LogConfig {
        public final String topic;
        public final String logBasePath;
        public final String logNamePattern;
        public final String encoding;
        public final boolean compress;
        public final boolean ordered;
        public final long timeout;
        public final String checkPointName;
        public final String tmpLogFullPath;


        public LogConfig(final String topic, final String logBasePath, final String logNamePattern,
                final String tmpLogFullPath, final String encoding, final boolean compress, final boolean ordered,
                final long timeout, final String checkPointName) {
            super();
            this.topic = topic;
            this.tmpLogFullPath = tmpLogFullPath;
            this.logBasePath = logBasePath;
            this.logNamePattern = logNamePattern;
            this.encoding = encoding;
            this.compress = compress;
            this.ordered = ordered;
            this.timeout = timeout;
            this.checkPointName = checkPointName;
        }


        @Override
        public String toString() {
            return "LogConfig [checkPointName=" + this.checkPointName + ", compress=" + this.compress + ", encoding="
                    + this.encoding + ", logBasePath=" + this.logBasePath + ", logNamePattern=" + this.logNamePattern
                    + ", ordered=" + this.ordered + ", timeout=" + this.timeout + ", topic=" + this.topic + "]";
        }

    }


    public static void main(final String[] args) throws Exception {
        final Config config = new Config();
        config.parseFromIni(ResourcesUtils.getResourceAsFile("tail4j.ini").getAbsolutePath());

        System.out.println(config);
    }
}

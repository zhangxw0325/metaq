package com.taobao.metamorphosis.server.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * 构建配置
 * 
 * @author boyan
 * @Date 2011-4-23
 * 
 */
public class BuildProperties {
    // 服务器版本
    public static String VERSION = "0.1";

    static {
        InputStream in = null;
        try {
            in = BuildProperties.class.getClassLoader().getResourceAsStream("build.properties");
            final Properties props = new Properties();
            props.load(in);
            VERSION = props.getProperty("version");
        }
        catch (final Throwable e) {

        }
        finally {
            if (in != null) {
                try {
                    in.close();
                }
                catch (final IOException e) {

                }
            }
        }
    }
}

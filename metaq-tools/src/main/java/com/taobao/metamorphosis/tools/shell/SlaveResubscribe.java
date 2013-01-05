package com.taobao.metamorphosis.tools.shell;

import java.io.PrintStream;
import java.io.PrintWriter;

import javax.management.ObjectInstance;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.taobao.metamorphosis.tools.utils.CommandLineUtils;
import com.taobao.metamorphosis.tools.utils.JMXClient;


/**
 * <pre>
 * 触发slave重启数据同步,在动态增加topic时用 
 * usage:
 *      SlaveResubscribe
 *      SlaveResubscribe -host 10.2.2.3
 *      SlaveResubscribe -port 9999
 *      SlaveResubscribe -host 10.2.2.3 -port 9999
 * </pre>
 * 
 * @author 无花
 * @since 2011-7-19 下午06:02:52
 */

public class SlaveResubscribe extends ShellTool {
    final static protected String HANDLE_NANE = "com.taobao.metamorphosis.metaslave:type=SubscribeHandler,*";


    public SlaveResubscribe(PrintStream out) {
        super(out);
    }


    public SlaveResubscribe(PrintWriter out) {
        super(out);
    }


    public static void main(String[] args) throws Exception {
        new SlaveResubscribe(System.out).doMain(args);
    }


    @Override
    public void doMain(String[] args) throws Exception {
        CommandLine commandLine =
                CommandLineUtils.parseCmdLine(args,
                    new Options().addOption("host", true, "host").addOption("port", true, "port"));

        String host = commandLine.getOptionValue("host", "127.0.0.1");
        int port = Integer.parseInt(commandLine.getOptionValue("port", "9999"));

        JMXClient jmxClient = JMXClient.getJMXClient(host, port);

        this.println("connected to " + jmxClient.getAddressAsString());

        ObjectInstance metaConfigInstance = jmxClient.queryMBeanForOne(HANDLE_NANE);
        if (metaConfigInstance != null) {
            jmxClient.invoke(metaConfigInstance.getObjectName(), "restart", new Object[0], new String[0]);
            jmxClient.close();
            this.println("invoke " + metaConfigInstance.getClassName() + "#restart success");
        }
        else {
            this.println("没有找到 " + HANDLE_NANE);
        }
    }
}

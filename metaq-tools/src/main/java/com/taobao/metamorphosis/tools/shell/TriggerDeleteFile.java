package com.taobao.metamorphosis.tools.shell;

import java.io.PrintStream;
import java.io.PrintWriter;

import javax.management.ObjectInstance;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.taobao.metamorphosis.tools.utils.CommandLineUtils;
import com.taobao.metamorphosis.tools.utils.JMXClient;

public class TriggerDeleteFile extends ShellTool {
	
	final static protected String METACONFIG_NAME = "com.taobao.metamorphosis.monitor:type=JmxManipulation,*";
	
	public TriggerDeleteFile(PrintStream out) {
        super(out);
    }


    public TriggerDeleteFile(PrintWriter out) {
        super(out);
    }


    public static void main(String[] args) throws Exception {
        new TriggerDeleteFile(System.out).doMain(args);
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
	    
	    ObjectInstance metaConfigInstance = jmxClient.queryMBeanForOne(METACONFIG_NAME);
	    
        if (metaConfigInstance != null) {
            jmxClient.invoke(metaConfigInstance.getObjectName(), "triggerDeleteFiles", new Object[0], new String[0]);
            jmxClient.close();
            this.println("invoke " + metaConfigInstance.getClassName() + "#trigger delete files success");
        }
        else {
            this.println("ц╩спур╣╫ " + METACONFIG_NAME);
        }
	}
}

package com.taobao.metamorphosis;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.metamorphosis.server.exception.MetamorphosisServerStartupException;
import com.taobao.metamorphosis.utils.Utils;


/**
 * @author ÎÞ»¨
 * @since 2011-6-9 ÏÂÎç03:45:45
 */

public class StartupHelp {
    static final Log log = LogFactory.getLog(StartupHelp.class);


    public static CommandLine parseCmdLine(String[] args, CommandLineParser parser) {
        return parseCmdLine(args, options(), parser);
    }


    public static CommandLine parseCmdLine(String[] args, Options options, CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        try {
            return parser.parse(options, args);
        }
        catch (ParseException e) {
            hf.printHelp("ServerStartup", options, true);
            log.error("Parse command line failed", e);
            throw new MetamorphosisServerStartupException("Parse command line failed", e);
        }
    }


    public static Options options() {
        Options options = new Options();
        Option brokerFile = new Option("f", true, "Broker configuration file path");
        brokerFile.setRequired(false);
        
        Option topicFile = new Option("t", true, "Topic configuration file path");
        topicFile.setRequired(false);

        options.addOption(brokerFile);
        options.addOption(topicFile);

        Option pluginParams =
                OptionBuilder.withArgName("pluginname=configfile").hasArgs(2).withValueSeparator()
                    .withDescription("use value for given param").create("F");
        options.addOption(pluginParams);

        return options;
    }


    public static Properties getProps(String path) {
        try {
            return Utils.getResourceAsProperties(path, "GBK");
        }
        catch (IOException e) {
            throw new MetamorphosisServerStartupException("Parse configuration failed,path=" + path, e);
        }
    }
}

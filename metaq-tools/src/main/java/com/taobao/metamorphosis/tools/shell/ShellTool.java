package com.taobao.metamorphosis.tools.shell;

import java.io.PrintStream;
import java.io.PrintWriter;


/**
 * 代表各种小工具
 * 
 * @author 无花
 * @since 2011-8-23 下午3:44:25
 */

public abstract class ShellTool {

    protected PrintWriter out;

    final static protected String METACONFIG_NAME = "com.taobao.metamorphosis.server.utils:type=MetaConfig,*";


    public ShellTool(PrintWriter out) {
        this.out = out;
    }


    public ShellTool(PrintStream out) {
        this.out = new PrintWriter(out);
    }


    /** 主功能入口 */
    abstract public void doMain(String[] args) throws Exception;


    protected void println(String x) {
        if (this.out != null) {
            this.out.println(x);
            this.out.flush();
        }
        else {
            System.out.println(x);
        }
    }

}

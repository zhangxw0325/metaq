package com.taobao.metamorphosis;

import java.util.Properties;

import org.junit.Test;

/**
 *
 * @author ÎÞ»¨
 * @since 2011-6-9 ÏÂÎç07:49:30
 */

public class StartupHelpTest {
    
    @Test
    public void testGetProps(){
        Properties props=StartupHelp.getProps("notify.properties");
        System.out.println(props.get("notify-groupId"));
    }

}

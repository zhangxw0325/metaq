package com.taobao.metamorphosis.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-6-22 ÏÂÎç04:11:30
 */

public class URIUtilsTest {

    @Test
    public void testParseParameters() throws URISyntaxException {
        Map<String, String> params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=true"), null);
        Assert.assertTrue(Boolean.parseBoolean(params.get("isSlave")));

        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=true&xx=yy&ww=qq"), null);
        Assert.assertTrue(Boolean.parseBoolean(params.get("isSlave")));

        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?xx=yy&ww=qq"), null);
        Assert.assertTrue(params.get("isSlave") == null);
        
        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=false&xx=yy&ww=qq"), null);
        Assert.assertFalse(Boolean.parseBoolean(params.get("isSlave")));
    }
    
    @Test
    public void testParseParameters_utf8() throws URISyntaxException {
        Map<String, String> params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=true"), "UTF-8");
        Assert.assertTrue(Boolean.parseBoolean(params.get("isSlave")));

        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=true&xx=yy&ww=qq"), "UTF-8");
        Assert.assertTrue(Boolean.parseBoolean(params.get("isSlave")));

        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?xx=yy&ww=qq"), "UTF-8");
        Assert.assertTrue(params.get("isSlave") == null);
        
        params = URIUtils.parseParameters(new URI("meta://10.10.2.2:8123?isSlave=false&xx=yy&ww=qq"), "UTF-8");
        Assert.assertFalse(Boolean.parseBoolean(params.get("isSlave")));
    }

}

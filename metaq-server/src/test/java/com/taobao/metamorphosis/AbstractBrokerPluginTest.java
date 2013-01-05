package com.taobao.metamorphosis;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.metamorphosis.server.assembly.MetaMorphosisBroker;

/**
 * @author ÎÞ»¨
 * @since 2011-6-9 ÏÂÎç03:09:39
 */

public class AbstractBrokerPluginTest {

    @Test
    public void testEquals() {
        Assert.assertFalse(new AbstractBrokerPluginAdapter() {
            public String name() {
                return null;
            }
        }.equals(null));

        Assert.assertTrue(new AbstractBrokerPluginAdapter() {
            public String name() {
                return null;
            }
        }.equals(new AbstractBrokerPluginAdapter() {
            public String name() {
                return null;
            }
        }));
        
        Assert.assertFalse(new AbstractBrokerPluginAdapter() {
            public String name() {
                return "";
            }
        }.equals(new AbstractBrokerPluginAdapter() {
            public String name() {
                return null;
            }
        }));
        
        Assert.assertFalse(new AbstractBrokerPluginAdapter() {
            public String name() {
                return null;
            }
        }.equals(new AbstractBrokerPluginAdapter() {
            public String name() {
                return "";
            }
        }));
        
        Assert.assertFalse(new AbstractBrokerPluginAdapter() {
            public String name() {
                return "ss";
            }
        }.equals(new AbstractBrokerPluginAdapter() {
            public String name() {
                return "sss";
            }
        }));
        
        Assert.assertTrue(new AbstractBrokerPluginAdapter() {
            public String name() {
                return "";
            }
        }.equals(new AbstractBrokerPluginAdapter() {
            public String name() {
                return "";
            }
        }));
        
        Assert.assertTrue(new AbstractBrokerPluginAdapter() {
            public String name() {
                return "qq";
            }
        }.equals(new AbstractBrokerPluginAdapter() {
            public String name() {
                return "qq";
            }
        }));
    }
    
    @Test
    public void testHashCode() {
        new AbstractBrokerPluginAdapter() {
            public String name() {
                return "qq";
            }
        }.hashCode();
        
        Assert.assertTrue(new AbstractBrokerPluginAdapter() {
            public String name() {
                return null;
            }
        }.hashCode()==0);
        
        Assert.assertTrue(new AbstractBrokerPluginAdapter() {
            public String name() {
                return "";
            }
        }.hashCode()==0);
    }

    private static class AbstractBrokerPluginAdapter extends AbstractBrokerPlugin {

        public void start() {
        }

        public void stop() {
        }

        public String name() {
            return null;
        }

        public void init(MetaMorphosisBroker metaMorphosisBroker, Properties props) {
        }

    }

}

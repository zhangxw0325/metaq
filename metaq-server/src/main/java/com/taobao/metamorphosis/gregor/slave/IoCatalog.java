package com.taobao.metamorphosis.gregor.slave;

import com.taobao.gecko.service.Connection;


/**
 * IO∑÷¿‡
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-8-18
 * 
 */
public final class IoCatalog {
    public final Connection connection;
    public final String catalog;


    public IoCatalog(final Connection connection, final String catalog) {
        super();
        this.connection = connection;
        this.catalog = catalog;
    }

}

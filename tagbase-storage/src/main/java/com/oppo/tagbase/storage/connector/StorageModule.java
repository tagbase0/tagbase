package com.oppo.tagbase.storage.connector;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.guice.ConfBind;
import com.oppo.tagbase.guice.PolyBind;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class StorageModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();
        ConfBind.bind(binder,
                "tagbase.bitmap.storage.hbase",
                HbaseStorageConnectorConfig.class
        );

        PolyBind.bind(
                binder,
                StorageConnector.class,
                "tagbase.bitmap.storage.type",
                "hbase"
        );

        PolyBind.registerImpl(
                binder,
                StorageConnector.class,
                "hbase",
                HbaseStorageConnector.class
        );
    }
}

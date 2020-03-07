package com.oppo.tagbase.storage.hbase;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.common.guice.ConfBind;
import com.oppo.tagbase.common.guice.ExtensionBind;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.connector.StorageConnectorConfig;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class HbaseStorageModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();

        ConfBind.bind(binder, HbaseStorageConnectorConfig.class);
        ConfBind.bind(binder, StorageConnectorConfig.class);
        ExtensionBind.bind(binder, StorageConnector.class);
        ExtensionBind.registerImpl(binder, HbaseStorageConnector.class);


//        ConfBind.bind(binder,
//                "tagbase.storage.hbase",
//                HbaseStorageConnectorConfig.class
//        );
//
//        ConfBind.bind(binder,
//                "tagbase.storage.core",
//                StorageConnectorConfig.class
//        );
//
//        ExtensionBind.bind(
//                binder,
//                StorageConnector.class,
//                "tagbase.storage.type",
//                "hbase"
//        );
//
//        ExtensionBind.registerImpl(
//                binder,
//                StorageConnector.class,
//                "hbase",
//                HbaseStorageConnector.class
//        );
    }
}

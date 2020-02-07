package com.oppo.tagbase.meta.connector;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.guice.ConfBind;
import com.oppo.tagbase.guice.PolyBind;

/**
 * Created by wujianchao on 2020/2/4.
 */
public class ConnectorModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();
        ConfBind.bind(binder,
                "tagbase.metadata.storage.com.oppo.tagbase.meta.connector",
                MetaStoreConnectorConfig.class
        );

        PolyBind.bind(
                binder,
                MetadataConnector.class,
                "tagbase.metadata.storage.type",
                "mysql"
        );

        PolyBind.registerImpl(
                binder,
                MetadataConnector.class,
                "mysql",
                MySQLMetadataConnector.class
        );
    }

}

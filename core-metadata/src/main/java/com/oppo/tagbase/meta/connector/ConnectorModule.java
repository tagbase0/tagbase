package com.oppo.tagbase.meta.connector;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.oppo.tagbase.common.guice.ConfBind;
import com.oppo.tagbase.common.guice.PolyBind;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.guava.GuavaPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

/**
 * Created by wujianchao on 2020/2/4.
 */
public class ConnectorModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();
        ConfBind.bind(binder, MetaStoreConnectorConfig.class);

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

    @Provides
    @Singleton
    private Jdbi getJdbi(MetaStoreConnectorConfig config) {
        Jdbi jdbi = Jdbi.create(
                config.getConnectURI(),
                config.getUser(),
                config.getPassword()
        );
        jdbi.installPlugin(new SqlObjectPlugin());
        jdbi.installPlugin(new GuavaPlugin());
        return jdbi;
    }

}

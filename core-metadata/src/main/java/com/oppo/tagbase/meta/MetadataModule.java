package com.oppo.tagbase.meta;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.oppo.tagbase.common.guice.ResourceBind;
import com.oppo.tagbase.meta.connector.ConnectorModule;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class MetadataModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new ConnectorModule());

        binder().bind(Metadata.class).in(Scopes.SINGLETON);
        ResourceBind.bind(binder(), MetadataResource.class);
    }
}

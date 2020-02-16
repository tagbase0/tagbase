package com.oppo.tagbase.storage.core.example.testobj;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class MetadataModule extends AbstractModule {

    @Override
    protected void configure() {

        binder().bind(Metadata.class).in(Scopes.SINGLETON);

    }
}

package com.oppo.tagbase.common.example.extension;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.common.guice.ExtensionBind;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class StorageModule extends AbstractModule {

    @Override
    protected void configure() {
        ExtensionBind.bind(binder(), Storage.class, "tagbase.example.storage.type", "hdfs");
    }
}

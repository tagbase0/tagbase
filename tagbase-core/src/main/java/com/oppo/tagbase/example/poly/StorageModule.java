package com.oppo.tagbase.example.poly;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.guice.PolyBind;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class StorageModule extends AbstractModule {

    @Override
    protected void configure() {
        PolyBind.bind(binder(), Storage.class, "tagbase.example.storage.type", "hdfs");
    }
}

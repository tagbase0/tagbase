package com.oppo.tagbase.example.storage;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.oppo.tagbase.guice.JsonConfigProvider;
import com.oppo.tagbase.guice.PolyBind;

/**
 * Created by wujianchao on 2020/1/19.
 */
public class HdfsModule extends AbstractModule {

    @Override
    protected void configure() {
        JsonConfigProvider.bind(binder(), "tagbase.example.storage", HdfsStorageConfig.class);

        PolyBind.optionBinder(binder(), Key.get(Storage.class))
                .addBinding("hdfs")
                .to(HdfsStorage.class);

    }
}

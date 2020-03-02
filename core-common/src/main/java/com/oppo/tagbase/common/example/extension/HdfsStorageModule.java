package com.oppo.tagbase.common.example.extension;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.common.guice.ConfBind;
import com.oppo.tagbase.common.guice.ExtensionBind;

/**
 * Created by wujianchao on 2020/1/19.
 */
public class HdfsStorageModule extends AbstractModule {

    @Override
    protected void configure() {
        ConfBind.bind(binder(), "tagbase.example.storage.hdfs", HdfsStorageConfig.class);
        ExtensionBind.registerImpl(binder(), Storage.class, "hdfs", HdfsStorage.class);
    }
}

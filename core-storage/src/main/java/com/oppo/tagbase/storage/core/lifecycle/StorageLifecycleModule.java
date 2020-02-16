package com.oppo.tagbase.storage.core.lifecycle;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.common.guice.Lifecycle;

/**
 * Created by liangjingya on 2020/2/10.
 */
public class StorageLifecycleModule extends AbstractModule {

    @Override
    protected void configure() {
        Lifecycle.registerInstance(binder(), StorageLifecycle.class);
    }
}

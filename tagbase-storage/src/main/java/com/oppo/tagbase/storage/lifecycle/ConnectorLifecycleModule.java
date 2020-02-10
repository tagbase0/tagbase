package com.oppo.tagbase.storage.lifecycle;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.guice.Lifecycle;

/**
 * Created by liangjingya on 2020/2/10.
 */
public class ConnectorLifecycleModule extends AbstractModule {

    @Override
    protected void configure() {
        Lifecycle.registerInstance(binder(), ConnectorLifecycle.class);
    }
}

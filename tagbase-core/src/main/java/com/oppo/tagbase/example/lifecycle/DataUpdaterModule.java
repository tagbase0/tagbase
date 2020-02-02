package com.oppo.tagbase.example.lifecycle;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.guice.Lifecycle;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class DataUpdaterModule extends AbstractModule {

    @Override
    protected void configure() {
        Lifecycle.registerInstance(binder(), DataUpdater.class);
    }
}

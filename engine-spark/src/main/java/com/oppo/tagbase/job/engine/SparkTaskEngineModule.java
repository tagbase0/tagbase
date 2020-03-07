package com.oppo.tagbase.job.engine;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.common.guice.ConfBind;
import com.oppo.tagbase.common.guice.ExtensionBind;
import com.oppo.tagbase.jobv2.spi.TaskEngine;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class SparkTaskEngineModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();

        ConfBind.bind(binder, SparkTaskConfig.class);
        ExtensionBind.bind(binder, TaskEngine.class);
        ExtensionBind.registerImpl(binder, SparkTaskEngine.class);
    }
}

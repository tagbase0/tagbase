package com.oppo.tagbase.job.engine.example.testobj;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.common.guice.ConfBind;
import com.oppo.tagbase.jobv2.JobConfig;

public class TestJobConfigModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();
        ConfBind.bind(binder, JobConfig.class);

    }
}

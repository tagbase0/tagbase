package com.oppo.tagbase.jobv2;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.common.guice.ConfBind;
import com.oppo.tagbase.common.guice.ExtensionBind;
import com.oppo.tagbase.common.guice.Lifecycle;
import com.oppo.tagbase.common.guice.ResourceBind;
import com.oppo.tagbase.jobv2.spi.TaskEngine;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();

        ConfBind.bind(binder, JobConfig.class);
        ConfBind.bind(binder, DictHiveInputConfig.class);

        binder.bind(JobManager.class);
        binder.bind(BatchBuildingJobMaker.class);
        binder.bind(DataJobExecutableMaker.class);
        binder.bind(DictJobExecutableMaker.class);

        ExtensionBind.bind(binder, Scheduler.class);
        ExtensionBind.registerImpl(binder, SingletonScheduler.class);
        ConfBind.bind(binder, SingletonSchedulerConfig.class);

        Lifecycle.registerInstance(binder, SingletonScheduler.class);

        ExtensionBind.bind(binder, TaskEngine.class);
        ResourceBind.bind(binder, JobResource.class);

    }
}

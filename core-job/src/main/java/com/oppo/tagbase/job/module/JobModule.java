package com.oppo.tagbase.job.module;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.common.guice.ResourceBind;
import com.oppo.tagbase.job.JobResource;
import com.oppo.tagbase.job.TaskEngine;
import com.oppo.tagbase.job.engine.SparkTaskEngine;

/**
 * Created by daikai on 2020/2/23.
 */
public class JobModule extends AbstractModule {


    @Override
    protected void configure() {

        ResourceBind.bind(binder(), JobResource.class);


        bind(TaskEngine.class).to(SparkTaskEngine.class);

    }

}

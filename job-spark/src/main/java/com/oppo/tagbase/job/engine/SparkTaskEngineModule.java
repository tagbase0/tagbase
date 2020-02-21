package com.oppo.tagbase.job.engine;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.common.guice.ConfBind;
import com.oppo.tagbase.common.guice.PolyBind;
import com.oppo.tagbase.job.TaskEngine;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class SparkTaskEngineModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();
        ConfBind.bind(binder,
                "tagbase.job.spark.bitmap",
                SparkJobConfig.class,
                "bitmapBuildingTaskConfig"
        );

        ConfBind.bind(binder,
                "tagbase.job.spark.invertedDict",
                SparkJobConfig.class,
                "invertedDictTaskConfig"
        );

        PolyBind.bind(
                binder,
                TaskEngine.class,
                "tagbase.job.type",
                "spark"
        );

        PolyBind.registerImpl(
                binder,
                TaskEngine.class,
                "spark",
                SparkTaskEngine.class
        );

    }
}

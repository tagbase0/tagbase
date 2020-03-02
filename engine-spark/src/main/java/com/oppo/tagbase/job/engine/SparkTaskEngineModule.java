package com.oppo.tagbase.job.engine;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.common.guice.ConfBind;
import com.oppo.tagbase.common.guice.ExtensionBind;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class SparkTaskEngineModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();
        ConfBind.bind(binder,
                "tagbase.job.spark.bitmap",
                SparkTaskConfig.class,
                "bitmapBuildingTaskConfig"
        );

        ConfBind.bind(binder,
                "tagbase.job.spark.invertedDict",
                SparkTaskConfig.class,
                "invertedDictTaskConfig"
        );

        ExtensionBind.bind(
                binder,
                TaskEngine.class,
                "tagbase.job.type",
                "spark"
        );

        ExtensionBind.registerImpl(
                binder,
                TaskEngine.class,
                "spark",
                SparkTaskEngine.class
        );

    }
}

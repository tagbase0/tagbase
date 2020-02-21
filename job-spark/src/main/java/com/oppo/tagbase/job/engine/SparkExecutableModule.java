package com.oppo.tagbase.job.engine;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.common.guice.ConfBind;
import com.oppo.tagbase.common.guice.PolyBind;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class SparkExecutableModule extends AbstractModule {

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
                AbstractExecutable.class,
                "tagbase.job.type",
                "spark"
        );

        PolyBind.registerImpl(
                binder,
                AbstractExecutable.class,
                "spark",
                SparkExecutable.class
        );

    }
}

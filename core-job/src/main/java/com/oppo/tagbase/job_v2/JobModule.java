package com.oppo.tagbase.job_v2;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.oppo.tagbase.common.guice.ConfBind;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobModule extends AbstractModule {

    @Override
    protected void configure() {

        Binder binder = binder();

        ConfBind.bind(binder, JobConfig.class);
        ConfBind.bind(binder, InvertedDictConfig.class);
        ConfBind.bind(binder, DictHiveInputConfig.class);


    }
}

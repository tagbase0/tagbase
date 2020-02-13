package com.oppo.tagbase.common.guice;

import com.google.inject.AbstractModule;

/**
 * Created by wujianchao on 2020/1/15.
 */
public class StatusModule extends AbstractModule {

    @Override
    protected void configure() {
        ResourceBind.bind(binder(), StatusResource.class);
    }

}

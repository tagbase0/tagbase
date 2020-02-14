package com.oppo.tagbase.server.module;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.common.guice.ResourceBind;
import com.oppo.tagbase.common.guice.StatusResource;

/**
 * Created by huangfeng on 2020/2/14.
 */
public class QueryModule extends AbstractModule{

    @Override
    protected void configure() {
        ResourceBind.bind(binder(), StatusResource.class);
    }


}

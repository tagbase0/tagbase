package com.oppo.tagbase.module;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.guice.ResourceBind;
import com.oppo.tagbase.guice.StatusResource;
import com.oppo.tagbase.server.QueryResource;

/**
 * Created by 71518 on 2020/2/7.
 */
public class QueryModule extends AbstractModule {
    @Override
    protected void configure() {
        ResourceBind.bind(binder(), QueryResource.class);
    }
}

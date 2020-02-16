package com.oppo.tagbase.server.module;

import com.google.inject.AbstractModule;
import com.oppo.tagbase.common.guice.ResourceBind;
import com.oppo.tagbase.query.IdGenerator;
import com.oppo.tagbase.query.QueryExecutionFactory;
import com.oppo.tagbase.server.QueryResource;

/**
 * Created by huangfeng on 2020/2/14.
 */
public class QueryModule extends AbstractModule {

    @Override
    protected void configure() {
        ResourceBind.bind(binder(), QueryResource.class);
        bind(IdGenerator.class);
        bind(QueryExecutionFactory.class);
        bind(Integer.class).toInstance(new Integer(1));

    }


}

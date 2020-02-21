package com.oppo.tagbase.query.module;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.oppo.tagbase.common.guice.ResourceBind;
import com.oppo.tagbase.query.*;
import com.oppo.tagbase.server.QueryResource;

/**
 * Created by huangfeng on 2020/2/14.
 */
public class QueryModule extends AbstractModule {


    @Override
    protected void configure() {
        ResourceBind.bind(binder(), QueryResource.class);


        bind(IdGenerator.class).in(Scopes.SINGLETON);
        bind(QueryExecutionFactory.class).in(Scopes.SINGLETON);
        bind(QueryManager.class).in(Scopes.SINGLETON);
        bind(QueryEngine.class).in(Scopes.SINGLETON);
        bind(SemanticAnalyzer.class).in(Scopes.SINGLETON);

        //ServerConfig
        bind(Integer.class).toInstance(new Integer(1));
    }


}

package com.oppo.tagbase.guice2;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.eclipse.jetty.server.Server;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class JettyModule extends AbstractModule {

    @Override
    protected void configure() {
        binder().bind(Server.class).in(Scopes.SINGLETON);
    }
}

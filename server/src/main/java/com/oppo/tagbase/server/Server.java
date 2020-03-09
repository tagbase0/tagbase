package com.oppo.tagbase.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.oppo.tagbase.common.guice.JettyModule;
import com.oppo.tagbase.common.guice.Lifecycle;
import com.oppo.tagbase.common.guice.LifecycleModule;
import com.oppo.tagbase.common.guice.ValidatorModule;
import com.oppo.tagbase.extension.spi.PluginManager;
import com.oppo.tagbase.meta.connector.ConnectorModule;
import com.oppo.tagbase.query.module.QueryModule;

import java.util.List;

public class Server {

    @Inject
    Injector baseInjector;


    public void run() throws Exception {
        Injector injector = makeInjectorWithModules(getModules());
        Lifecycle lifecycle = injector.getInstance(Lifecycle.class);
        lifecycle.start();
        System.out.println("start");

        lifecycle.join();
    }


    public Injector makeInjectorWithModules(List<Module> modules) throws Exception {

        PluginManager pluginManager = baseInjector.getInstance(PluginManager.class);

        List<Module> pluginModules = pluginManager.load("plugin");

        return Guice.createInjector(Modules.override(modules).with(pluginModules));

    }



    private List<Module> getModules(){
        return ImmutableList.of(new JettyModule(),
                new LifecycleModule(),
                new ValidatorModule(),
                new QueryModule(),
                new ConnectorModule());

    }
}

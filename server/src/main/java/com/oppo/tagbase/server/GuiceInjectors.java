package com.oppo.tagbase.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.oppo.tagbase.common.guice.*;

import java.util.Collection;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class GuiceInjectors {

    public static Injector makeInjector(Module... module) {
        return Guice.createInjector(module);
    }

    public static Injector makeStartupInjector() {
        return Guice.createInjector(defaultStartupModules());
    }

    public static Collection<Module> defaultStartupModules() {
        return ImmutableList.of(
                new JacksonModule(),
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new ValidatorModule(),
                new LifecycleModule(),
                new JettyModule(),
                new StatusModule()
        );
    }
}

package com.oppo.tagbase.common.guice;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class ExampleGuiceInjectors {

    public static Injector makeInjector(Module... modules) {
        return Guice.createInjector(modules);
    }

}

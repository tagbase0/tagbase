package com.oppo.tagbase.guice2;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class GuiceInjectors {

    public static Injector makeInjector(Module... module){
        return Guice.createInjector(module);
    }
}

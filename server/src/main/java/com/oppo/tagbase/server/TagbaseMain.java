package com.oppo.tagbase.server;

import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.JettyModule;
import com.oppo.tagbase.common.guice.Lifecycle;
import com.oppo.tagbase.common.guice.LifecycleModule;
import com.oppo.tagbase.common.guice.ValidatorModule;
import com.oppo.tagbase.query.module.QueryModule;

/**
 * Just for test.
 * <p>
 * Created by wujianchao on 2020/1/15.
 */
public class TagbaseMain {

    public static void main(String[] args) throws InterruptedException {


        Injector ij = GuiceInjectors.makeInjector(
                new JettyModule(),
                new LifecycleModule(),
                new ValidatorModule(),
                new QueryModule()
        );
        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);
        lifecycle.start();
        System.out.println("start");
        lifecycle.join();
    }
}

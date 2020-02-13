package com.oppo.tagbase.common.example.lifecycle;

import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.ExampleGuiceInjectors;
import com.oppo.tagbase.common.guice.Lifecycle;
import com.oppo.tagbase.common.guice.LifecycleModule;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class LifecycleExample {

    public static void main(String[] args) {
        Injector ij = ExampleGuiceInjectors.makeInjector(
                new LifecycleModule(),
                new DataUpdaterModule()
        );
        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);
        lifecycle.start();
    }
}

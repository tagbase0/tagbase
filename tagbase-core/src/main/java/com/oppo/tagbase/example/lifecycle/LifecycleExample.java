package com.oppo.tagbase.example.lifecycle;

import com.google.inject.Injector;
import com.oppo.tagbase.guice.GuiceInjectors;
import com.oppo.tagbase.guice.Lifecycle;
import com.oppo.tagbase.guice.LifecycleModule;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class LifecycleExample {

    public static void main(String[] args) {
        Injector ij = GuiceInjectors.makeInjector(
                new LifecycleModule(),
                new DataUpdaterModule()
        );
        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);
        lifecycle.start();
        lifecycle.stop();
    }
}

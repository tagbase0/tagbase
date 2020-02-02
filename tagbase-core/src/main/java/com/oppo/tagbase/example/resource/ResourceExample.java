package com.oppo.tagbase.example.resource;

import com.google.inject.Injector;
import com.oppo.tagbase.guice.*;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class ResourceExample {

    public static void main(String[] args) {
        Injector ij = GuiceInjectors.makeInjector(
                new JettyModule(),
                new LifecycleModule(),
                new StatusModule()
        );
        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);
        lifecycle.start();
    }
}

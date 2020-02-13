package com.oppo.tagbase.server;

import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.ExampleGuiceInjectors;
import com.oppo.tagbase.common.guice.Lifecycle;

/**
 * Just for test.
 * <p>
 * Created by wujianchao on 2020/1/15.
 */
public class TagbaseMain {

    public static void main(String[] args) throws InterruptedException {

        Injector ij = ExampleGuiceInjectors.makeInjector();

        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);

        lifecycle.start();

        System.out.println("start");
        lifecycle.join();
    }
}

package com.oppo.tagbase;

import com.google.inject.Injector;
import com.oppo.tagbase.guice.GuiceInjectors;
import com.oppo.tagbase.guice.Lifecycle;

/**
 * Just for test.
 * <p>
 * Created by wujianchao on 2020/1/15.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {

        Injector ij = GuiceInjectors.makeStartupInjector();

        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);

        lifecycle.start();

        System.out.println("start");
        lifecycle.join();
    }
}

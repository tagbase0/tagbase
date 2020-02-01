package com.oppo.tagbase.guice2;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.oppo.tagbase.example.lifecycle.DataUpdater;
import com.oppo.tagbase.example.storage.HdfsStorageConfig;
import org.junit.Test;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class LifecycleTest {

    @Test
    public void sanityTest(){
        Injector ij = GuiceInjectors.makeInjector(
                new LifecycleModule(){
                    @Override
                    protected void configure() {
                        super.configure();
                        Lifecycle.registerHandler(binder(), DataUpdater.class);
                    }
                }
        );

        ij.getInstance(Lifecycle.class).start();
    }

}

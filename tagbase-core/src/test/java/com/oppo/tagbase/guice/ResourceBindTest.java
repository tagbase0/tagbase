package com.oppo.tagbase.guice;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.oppo.tagbase.example.poly.HdfsStorageConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class ResourceBindTest {

    @Test
    public void sanityTest() {
        Injector ij = GuiceInjectors.makeInjector(
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        ConfBind.bind(binder(), "tagbase.example.storage", HdfsStorageConfig.class);
                    }
                }
        );

        Assert.assertEquals("/path/to/storage",
                ij.getInstance(HdfsStorageConfig.class).getPath());
    }
}

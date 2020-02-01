package com.oppo.tagbase.guice2;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.oppo.tagbase.example.storage.HdfsStorageConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class ResourcesTest {

    @Test
    public void sanityTest(){
        Injector ij = GuiceInjectors.makeInjector(
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        Configs.bind(binder(), "tagbase.example.storage", HdfsStorageConfig.class);
                    }
                }
        );

        Assert.assertEquals("/path/to/storage",
                ij.getInstance(HdfsStorageConfig.class).getPath());
    }
}

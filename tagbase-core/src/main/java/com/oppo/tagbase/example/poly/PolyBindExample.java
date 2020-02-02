package com.oppo.tagbase.example.poly;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.oppo.tagbase.guice.GuiceInjectors;
import com.oppo.tagbase.guice.JacksonModule;
import com.oppo.tagbase.guice.PropsModule;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class PolyBindExample {

    public static void main(String[] args) {
        Injector ij = GuiceInjectors.makeInjector(
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new HdfsStorageModule(),
                new S3StorageModule(),
                new StorageModule()
        );

        Storage storage = ij.getInstance(Storage.class);
        storage.push(null);
    }
}

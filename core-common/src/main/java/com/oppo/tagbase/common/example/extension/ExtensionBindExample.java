package com.oppo.tagbase.common.example.extension;

import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.ExampleGuiceInjectors;
import com.oppo.tagbase.common.guice.ValidatorModule;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class ExtensionBindExample {

    public static void main(String[] args) {
        Injector ij = ExampleGuiceInjectors.makeInjector(
                new ValidatorModule(),
                new StorageModule(),
                new HdfsStorageModule(),
                new S3StorageModule()
        );

        Storage storage = ij.getInstance(Storage.class);
        storage.push(null);
    }
}

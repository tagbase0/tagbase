package com.oppo.tagbase.common.example.conf;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.oppo.tagbase.common.example.extension.*;
import com.oppo.tagbase.common.guice.ExampleGuiceInjectors;
import com.oppo.tagbase.common.guice.JacksonModule;
import com.oppo.tagbase.common.guice.PropsModule;
import com.oppo.tagbase.common.guice.ValidatorModule;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class ConfBindExample {

    public static void main(String[] args) {
        Injector ij = ExampleGuiceInjectors.makeInjector(
                new ValidatorModule(),
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new StorageModule(),
                new HdfsStorageModule(),
                new S3StorageModule()
        );

        HdfsStorageConfig hdfsStorageConfig = ij.getInstance(HdfsStorageConfig.class);
        System.out.println(hdfsStorageConfig.getPath());

        try {
            S3StorageConfig s3 = ij.getInstance(S3StorageConfig.class);
            System.out.println(s3.getBucket());
        } catch (Exception e) {
            System.out.println(e instanceof ProvisionException);
        }
    }
}

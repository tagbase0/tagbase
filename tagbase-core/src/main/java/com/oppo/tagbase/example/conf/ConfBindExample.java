package com.oppo.tagbase.example.conf;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.oppo.tagbase.example.poly.HdfsStorageConfig;
import com.oppo.tagbase.example.poly.HdfsStorageModule;
import com.oppo.tagbase.example.poly.S3StorageConfig;
import com.oppo.tagbase.example.poly.S3StorageModule;
import com.oppo.tagbase.guice.GuiceInjectors;
import com.oppo.tagbase.guice.JacksonModule;
import com.oppo.tagbase.guice.PropsModule;
import com.oppo.tagbase.guice.ValidatorModule;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class ConfBindExample {

    public static void main(String[] args) {
        Injector ij = GuiceInjectors.makeInjector(
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new ValidatorModule(),
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

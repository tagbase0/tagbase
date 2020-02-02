package com.oppo.tagbase.example.poly;

import com.google.inject.Inject;

/**
 * Created by wujianchao on 2020/1/19.
 */
public class S3Storage implements Storage {

    @Inject
    private S3StorageConfig config;


    @Override
    public void push(byte[] data) {
        System.out.println("s3 storage push some data");
    }
}

package com.oppo.tagbase.example.poly;

import com.google.inject.Inject;

/**
 * Created by wujianchao on 2020/1/19.
 */
public class HdfsStorage implements Storage {

    @Inject
    private HdfsStorageConfig config;


    @Override
    public void push(byte[] data) {
        System.out.println("hdfs storage push some data");
    }
}

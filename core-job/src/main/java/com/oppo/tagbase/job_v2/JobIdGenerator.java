package com.oppo.tagbase.job_v2;

import java.util.UUID;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobIdGenerator {

    public static String nextId() {
        return UUID.randomUUID().toString();
    }

    public static void main(String[] args) {
        System.out.println(nextId());
    }
}

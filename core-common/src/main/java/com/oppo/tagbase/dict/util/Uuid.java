package com.oppo.tagbase.dict.util;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class Uuid {

    public static String nextId() {
        return java.util.UUID.randomUUID().toString();
    }

    public static void main(String[] args) {
        System.out.println(nextId());
    }
}

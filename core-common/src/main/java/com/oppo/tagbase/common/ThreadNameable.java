package com.oppo.tagbase.common;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by wujianchao on 2020/2/29.
 */
public class ThreadNameable implements Closeable {

    private String previousName;

    public ThreadNameable(String name) {
        Thread.currentThread().setName(name);
        this.previousName = Thread.currentThread().getName();
    }

    public static ThreadNameable of(String name) {
        return new ThreadNameable(name);
    }

    @Override
    public void close() throws IOException {
        Thread.currentThread().setName(previousName);
    }
}

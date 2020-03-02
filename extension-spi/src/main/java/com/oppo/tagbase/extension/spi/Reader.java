package com.oppo.tagbase.extension.spi;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author huangfeng
 * @date 2020/3/2 14:30
 */
public interface Reader extends Closeable {
    byte[] readFully() throws IOException;
}

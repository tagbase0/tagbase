package com.oppo.tagbase.extension.spi;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author huangfeng
 * @date 2020/2/28 14:14
 */
public interface ResultWriter extends Closeable {

    void write(String content) throws IOException;
}

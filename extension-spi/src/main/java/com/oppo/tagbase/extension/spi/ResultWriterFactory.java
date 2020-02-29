package com.oppo.tagbase.extension.spi;

import java.io.IOException;

/**
 * @author huangfeng
 * @date 2020/2/28 19:01
 */
public interface ResultWriterFactory {
    ResultWriter createResultWriter(String filePath) throws IOException;
    String getName();
}

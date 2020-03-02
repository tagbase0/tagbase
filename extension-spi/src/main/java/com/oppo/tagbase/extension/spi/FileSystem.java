package com.oppo.tagbase.extension.spi;

import java.io.IOException;

/**
 * @author huangfeng
 * @date 2020/2/28 19:01
 */
public interface FileSystem {
    Writer createWriter(String filePath) throws IOException;

    Reader createReader(String filePath) throws IOException;

    String getName();
}

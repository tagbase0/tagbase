package com.oppo.tagbase.extension.spi;

import java.io.IOException;

/**
 * @author huangfeng
 * @date 2020/3/3 10:47
 */
public interface FileSystemFactory {
    FileSystem createFileSystem()throws IOException;
    String getName();
}

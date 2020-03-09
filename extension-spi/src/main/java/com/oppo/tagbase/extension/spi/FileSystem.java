package com.oppo.tagbase.extension.spi;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * @author huangfeng
 * @date 2020/2/28 19:01
 */
public interface FileSystem extends Closeable {
    OutputStream create(String filePath) throws IOException;

    InputStream open(String filePath) throws IOException;

    void copyFromLocalFile(String srcPath, String destPath) throws IOException;

    void copyToLocalFile(String srcPath, String destPath) throws IOException;

    List<String> listFiles(String dirPath) throws  IOException;

    long getFileSize(String filePath) throws IOException;

    String getName();
}

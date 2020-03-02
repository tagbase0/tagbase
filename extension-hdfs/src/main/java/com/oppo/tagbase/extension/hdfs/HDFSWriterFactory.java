package com.oppo.tagbase.extension.hdfs;

import com.oppo.tagbase.extension.spi.FileSystem;
import com.oppo.tagbase.extension.spi.Reader;
import com.oppo.tagbase.extension.spi.Writer;

import java.io.IOException;

/**
 * @author huangfeng
 * @date 2020/2/28 18:58
 */
public class HDFSWriterFactory implements FileSystem {

    @Override
    public Writer createWriter(String filePath) throws IOException {
      return   new HDFSWriter(filePath);
    }

    @Override
    public Reader createReader(String filePath) throws IOException {
        return new HDFSReader(filePath);
    }

    @Override
    public String getName() {
        return "hdfs";
    }
}

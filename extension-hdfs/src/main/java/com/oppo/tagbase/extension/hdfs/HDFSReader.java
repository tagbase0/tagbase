package com.oppo.tagbase.extension.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author huangfeng
 * @date 2020/3/2 14:30
 */
public class HDFSReader extends InputStream {
    FSDataInputStream fsd;

    public HDFSReader(FSDataInputStream fsd) throws IOException {
        this.fsd = fsd;
    }

    @Override
    public int read() throws IOException {
        return fsd.read();
    }

    @Override
    public void close() throws IOException {
        if (fsd != null) {
            fsd.close();
        }
    }
}

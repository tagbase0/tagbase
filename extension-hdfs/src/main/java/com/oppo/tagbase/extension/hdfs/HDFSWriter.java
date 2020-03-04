package com.oppo.tagbase.extension.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author huangfeng
 * @date 2020/2/28 14:42
 */
public class HDFSWriter extends OutputStream {

    FSDataOutputStream fsd;

    public HDFSWriter(FSDataOutputStream fsd) throws IOException {
        this.fsd = fsd;
    }


    @Override
    public void write(int b) throws IOException {
        fsd.write(b);
    }

    @Override
    public void close() throws IOException {

        if (fsd != null) {
            fsd.close();
        }
    }
}

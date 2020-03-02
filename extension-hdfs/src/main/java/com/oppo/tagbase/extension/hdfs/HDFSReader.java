package com.oppo.tagbase.extension.hdfs;

import com.oppo.tagbase.extension.spi.Reader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * @author huangfeng
 * @date 2020/3/2 14:30
 */
public class HDFSReader implements Reader {
    FileSystem fs;
    FSDataInputStream fsd;
    ByteArrayOutputStream bos;
    boolean hasRead = false;

    public HDFSReader(String path) throws IOException {
        Configuration configuration = new Configuration();
        fs = FileSystem.newInstance(configuration);
        fsd = fs.open(new Path(path));
        bos = new ByteArrayOutputStream();


    }

     public byte[] readFully() throws IOException {
        if (hasRead) {
            return bos.toByteArray();
        }
        byte[] buffer = new byte[1024];
        int len = -1;
        while ((len = fsd.read(buffer)) != -1) {
            bos.write(buffer, 0, len);
        }
        hasRead = true;
        return bos.toByteArray();
    }


    @Override
    public void close() throws IOException {
        try {
            if (bos != null) {
                bos.close();
            }
        } finally {
            try {
                if (fsd != null) {
                    fsd.close();
                }
            } finally {
                // exception throws after fs close fail
                if (fs != null) {
                    fs.close();
                }
            }
        }
    }
}

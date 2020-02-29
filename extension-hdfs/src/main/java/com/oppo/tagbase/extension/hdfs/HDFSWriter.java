package com.oppo.tagbase.extension.hdfs;

import com.oppo.tagbase.extension.spi.ResultWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * @author huangfeng
 * @date 2020/2/28 14:42
 */
public class HDFSWriter implements ResultWriter {

    FileSystem fs;
    FSDataOutputStream fsd;

    BufferedWriter bw;

    public HDFSWriter(String path) throws IOException {
        Configuration configuration = new Configuration();
        fs = FileSystem.newInstance(configuration);

        fsd = fs.create(new Path(path));

        bw = new BufferedWriter(new OutputStreamWriter(fsd, "UTF-8"));

    }

    @Override
    public void write(String content) throws IOException {
        bw.write(content);
    }

    @Override
    public void close() throws IOException {
        try {
            if (bw != null) {
                bw.close();
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

package com.oppo.tagbase.extension.hdfs;

import com.oppo.tagbase.extension.spi.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author huangfeng
 * @date 2020/2/28 18:58
 */
public class HDFSFileSystem implements FileSystem {
    org.apache.hadoop.fs.FileSystem delegate;

    public HDFSFileSystem(Configuration configuration) throws IOException {
        delegate = org.apache.hadoop.fs.FileSystem.newInstance(configuration);
    }

    @Override
    public OutputStream create(String filePath) throws IOException {
        FSDataOutputStream fsd = delegate.create(new Path(filePath));
        return new HDFSWriter(fsd);
    }

    @Override
    public InputStream open(String filePath) throws IOException {
        FSDataInputStream fsd = delegate.open(new Path(filePath));
        return new HDFSReader(fsd);
    }

    public void copyFromLocalFile(String srcPath,String destPath) throws IOException {
        delegate.copyFromLocalFile(new Path(srcPath),new Path(destPath));
    }

    public void copyToLocalFile(String srcPath,String destPath) throws IOException {
        delegate.copyToLocalFile(new Path(srcPath),new Path(destPath));
    }


    @Override
    public long getFileSize(String filePath) throws IOException {
        return delegate.getFileStatus(new Path(filePath)).getLen();
    }

    @Override
    public String getName() {
        return "hdfs";
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}

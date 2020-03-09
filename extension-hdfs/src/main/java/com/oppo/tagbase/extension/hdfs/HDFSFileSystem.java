package com.oppo.tagbase.extension.hdfs;

import com.google.inject.Inject;
import com.oppo.tagbase.extension.spi.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author huangfeng
 * @date 2020/2/28 18:58
 */
public class HDFSFileSystem implements FileSystem {
    org.apache.hadoop.fs.FileSystem delegate;


    @Inject
    public HDFSFileSystem(Configuration conf) throws IOException {
        conf.setClassLoader(HDFSFileSystem.class.getClassLoader());
        delegate = org.apache.hadoop.fs.FileSystem.get(conf);
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

    @Override
    public void copyFromLocalFile(String srcPath, String destPath) throws IOException {
        delegate.copyFromLocalFile(new Path(srcPath), new Path(destPath));
    }

    @Override
    public void copyToLocalFile(String srcPath, String destPath) throws IOException {
        delegate.copyToLocalFile(new Path(srcPath), new Path(destPath));
    }

    @Override
    public List<String> listFiles(String dirPath) throws IOException {
        return Arrays.stream(delegate.listStatus(new Path(dirPath))).map(fileStatus -> fileStatus.getPath().getName()).collect(Collectors.toList());
    }


    @Override
    public long getFileSize(String filePath) throws IOException {
        Path path = new Path(filePath);
        System.out.println(path);
        FileStatus status = delegate.getFileStatus(new Path(filePath));
        System.out.println(status);
        return status.getLen();
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

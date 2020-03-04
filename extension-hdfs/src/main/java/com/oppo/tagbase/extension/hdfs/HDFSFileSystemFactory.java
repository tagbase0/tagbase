package com.oppo.tagbase.extension.hdfs;

import com.oppo.tagbase.extension.spi.FileSystemFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * @author huangfeng
 * @date 2020/3/3 10:47
 */
public class HDFSFileSystemFactory implements FileSystemFactory {

    public HDFSFileSystem createFileSystem() throws IOException {
        // ensure configuration file is in classpath
        Configuration configuration = new Configuration();
        return new HDFSFileSystem(configuration);
    }

    @Override
    public String getName() {
        return "hdfs";
    }
}

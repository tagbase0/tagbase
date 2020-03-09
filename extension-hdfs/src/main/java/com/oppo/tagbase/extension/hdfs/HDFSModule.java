package com.oppo.tagbase.extension.hdfs;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.oppo.tagbase.extension.spi.FileSystem;
import org.apache.hadoop.conf.Configuration;

public class HDFSModule implements Module {

    @Override
    public void configure(Binder binder) {

        final Configuration conf = new Configuration();
        conf.setClassLoader(getClass().getClassLoader());

        binder.bind(Configuration.class).toInstance(conf);
        binder.bind(FileSystem.class).to(HDFSFileSystem.class).in(Scopes.SINGLETON);

    }

}

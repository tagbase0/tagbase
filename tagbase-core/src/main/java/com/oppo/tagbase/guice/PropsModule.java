package com.oppo.tagbase.guice;

import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Load configuration file.
 * <p>
 * Created by wujianchao on 2020/1/21.
 */
public class PropsModule extends AbstractModule {

    private List<String> configFileList;

    private Properties props = new Properties();

    public PropsModule(List<String> configFileList) {
        this.configFileList = configFileList;
    }

    @Override
    protected void configure() {
        parseProps();
        binder().bind(Properties.class).toInstance(props);
    }

    private void parseProps() {
        configFileList.forEach((configFile) -> {
            try {
                props.load(ClassLoader.getSystemResourceAsStream(configFile));
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        });
    }
}

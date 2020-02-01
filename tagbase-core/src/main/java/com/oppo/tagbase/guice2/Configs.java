package com.oppo.tagbase.guice2;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Created by wujianchao on 2020/1/20.
 */
public class Configs {


    public static <T> void bind(Binder binder, String confKeyPrefix, Class<T> configClazz){
        binder.bind(configClazz).toProvider(new ConfigProvider<T>(confKeyPrefix, configClazz)).in(Scopes.SINGLETON);
    }

    static class ConfigProvider<T> implements Provider {

        private final String confKeyPrefix;
        private final Class<T> configClazz;

        ConfigProvider(String confKeyPrefix, Class<T> configClazz){
            this.confKeyPrefix = confKeyPrefix;
            this.configClazz = configClazz;
        }

        private Properties props;
        private ObjectMapper om;

        @Inject
        private void inject(Properties props, ObjectMapper om){
            this.om = om;
            this.props = props;
        }

        @Override
        public Object get() {

            Map<String, String> configEntries = props.keySet().stream()
                    .map(Object::toString)
                    .filter(key -> key.startsWith(confKeyPrefix))
                    .collect(Collectors.toMap(
                            key -> key.substring(confKeyPrefix.length() + 1),
                            props::getProperty)
                    );

            return om.convertValue(configEntries, configClazz);
        }
    }

}

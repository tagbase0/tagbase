package com.oppo.tagbase.common.guice;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class JacksonModule extends AbstractModule {

    @Override
    protected void configure() {
        ObjectMapper om = new ObjectMapper();
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        binder().bind(ObjectMapper.class).toInstance(om);
    }

}

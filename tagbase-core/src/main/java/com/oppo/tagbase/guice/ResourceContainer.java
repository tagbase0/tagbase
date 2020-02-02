package com.oppo.tagbase.guice;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.WebConfig;

import java.util.Map;
import java.util.Set;

/**
 * Created by wujianchao on 2020/1/15.
 */
@Singleton
public class ResourceContainer extends GuiceContainer {

    private final Set<Class<?>> resources;

    @Inject
    public ResourceContainer(
            Injector injector,
            @Resource Set<Class<?>> resources
    ) {
        super(injector);
        this.resources = resources;
        System.out.println(resources);
    }

    @Override
    protected ResourceConfig getDefaultResourceConfig(
            Map<String, Object> props, WebConfig webConfig
    ) {
        return new DefaultResourceConfig(resources);
    }

}

package com.oppo.tagbase.common.guice;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Created by wujianchao on 2020/2/22.
 */
@Target({ElementType.TYPE})
@Retention(RUNTIME)
public @interface Config {

    /**
     * Identify which module the config serve.
     * @return module prefix in configuration file
     */
    String value();

}

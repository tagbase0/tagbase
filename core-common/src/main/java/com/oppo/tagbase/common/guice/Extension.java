package com.oppo.tagbase.common.guice;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Used to interface to identify which implementation Guice will return
 *
 * Created by wujianchao on 2020/2/22.
 */
@Target({ElementType.TYPE})
@Retention(RUNTIME)
public @interface Extension {

    /**
     * configuration key which identifying the implementation name.
     */
    String key();

    /**
     * default implementation
     */
    String defaultImpl();

}

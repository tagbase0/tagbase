package com.oppo.tagbase.common.guice;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Naming for poly implementation
 *
 * Created by wujianchao on 2020/2/29.
 */
@Target({ElementType.TYPE})
@Retention(RUNTIME)
public @interface PolyName {
    String name();
}
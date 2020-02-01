package com.oppo.tagbase.guice2;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Created by wujianchao on 2020/1/21.
 */
@Target({ElementType.METHOD})
@Retention(RUNTIME)
public @interface LifecycleStop {

}

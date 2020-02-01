package com.oppo.tagbase.guice2;

import com.google.inject.ScopeAnnotation;

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Created by wujianchao on 2020/1/21.
 */
@Retention(RUNTIME)
@ScopeAnnotation
public @interface LifecycleBinding {
    String value = "";
}

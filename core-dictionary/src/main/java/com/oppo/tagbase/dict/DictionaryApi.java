package com.oppo.tagbase.dict;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * public api mark
 *
 * Created by wujianchao on 2020/2/20.
 */
@Target({ElementType.METHOD})
@Retention(RUNTIME)
public @interface DictionaryApi {
}

package com.oppo.tagbase.common.util;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.core.PackagesResourceConfig;

import java.lang.annotation.Annotation;

import static java.lang.String.format;

/**
 * Created by wujianchao on 2020/2/22.
 */
public class AnnotationUtil {

    public static final <A extends Annotation> A getAnnotation(Class<?> clazz, Class<A> annClazz) {
        A a = clazz.getAnnotation(annClazz);
        return Preconditions.checkNotNull(a,
                format("Class %s has no Annotation of %s", clazz.getName(), annClazz.getName()));
    }

}

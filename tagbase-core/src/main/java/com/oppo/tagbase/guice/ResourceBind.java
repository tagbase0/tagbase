package com.oppo.tagbase.guice;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

/**
 * Created by wujianchao on 2020/1/20.
 */
public class ResourceBind {

    public static void bind(Binder binder, Class<?> resourceClazz) {
        Multibinder.newSetBinder(binder, new TypeLiteral<Class<?>>() {
        }, Resource.class)
                .addBinding().toInstance(resourceClazz);
    }
}

package com.oppo.tagbase.guice2;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

/**
 * Created by wujianchao on 2020/1/20.
 */
public class Resources {

    public static void addResource(Binder binder, Class<?> resourceClazz){
        Multibinder.newSetBinder(binder, new TypeLiteral<Class<?>>(){}, Resource.class)
                .addBinding().toInstance(resourceClazz);
    }
}

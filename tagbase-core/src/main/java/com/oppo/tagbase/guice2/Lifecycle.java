package com.oppo.tagbase.guice2;

import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Set;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class Lifecycle {

    public static void registerHandler(Binder binder, Class handler){
        Multibinder.newSetBinder(binder, Key.get(KeyHolder.class, Names.named("lifecycle")))
                .addBinding().toInstance(new KeyHolder<Object>(Key.get(handler)));
    }

    private Set<Object> handlers;

    @Inject
    public void inject(Set<Object> handlers){
        this.handlers = handlers;
    }

    public void addManagedInstance(Object instance){
        handlers.add(instance);
    }

    public void start(){
        for (Object handler : handlers) {
            new Handler(handler).stop();
        }
    }

    public void stop(){
        for (Object handler : handlers) {
            new Handler(handler).stop();
        }
    }

    public static <T extends Annotation> void invokeAnnotatedMethod(Object o, Class<T> annotationClass){
        try {
            Class clazz = o.getClass();

            for (Method method : clazz.getMethods()) {
                if (method.isAnnotationPresent(annotationClass)) {
                    method.invoke(o);
                }
            }
        }catch (Exception e){
            Throwables.propagate(e);
        }
    }

    static class Handler {
        private Object instance;

        Handler(Object instance){
            this.instance = instance;
        }

        public void start(){
            invokeAnnotatedMethod(instance, LifecycleStart.class);
        }

        public void stop(){
            invokeAnnotatedMethod(instance, LifecycleStop.class);
        }

    }

    static class KeyHolder<T>
    {
        private final Key<? extends T> key;

        public KeyHolder(
                Key<? extends T> key
        )
        {
            this.key = key;
        }

        public Key<? extends T> getKey()
        {
            return key;
        }
    }


}

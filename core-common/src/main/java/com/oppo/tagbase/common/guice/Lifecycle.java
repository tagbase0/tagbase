package com.oppo.tagbase.common.guice;

import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by wujianchao on 2020/1/21.
 */
public final class Lifecycle {

    private static Logger LOG = LoggerFactory.getLogger(Lifecycle.class);

    // TODO Adding registering interfaces support
    public static void registerInstance(Binder binder, Class<?> instance) {
        Multibinder.newSetBinder(binder, new TypeLiteral<Class<?>>() {
        }, Names.named("lifecycle"))
                .addBinding().toInstance(instance);
    }

    private Set<Object> instances = new HashSet<>();

    /**
     * Adding managed instance into Lifecycle.
     * Warning: This is not a user API.
     *
     * @param instance
     */
    public void addManagedInstance(Object instance) {
        instances.add(instance);
    }


    public void start() {

        for (Object instance : instances) {
            LOG.debug("Start - {}", instance.getClass().getSimpleName());
            Handler.wrap(instance).start();
        }
    }

    public void join() throws InterruptedException {
        addShutdownHook();
        Thread.currentThread().join();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop();
        }, "shutdown-hook"));
    }

    private void stop() {
        for (Object handler : instances) {
            Handler.wrap(handler).stop();
        }
    }

    private static <T extends Annotation> void invokeAnnotatedMethod(Object o, Class<T> annotationClass) {
        try {
            Class clazz = o.getClass();

            // TODO support interface annotated invoking
            for (Method method : clazz.getMethods()) {
                if (method.isAnnotationPresent(annotationClass)) {
                    method.invoke(o);
                }
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    static final class Handler {
        private Object instance;

        private Handler(Object instance) {
            this.instance = instance;
        }

        public static Handler wrap(Object instance){
            return new Handler(instance);
        }

        public void start() {
            invokeAnnotatedMethod(instance, LifecycleStart.class);
        }

        public void stop() {
            invokeAnnotatedMethod(instance, LifecycleStop.class);
        }

    }


}

package com.oppo.tagbase.guice2;

import com.google.common.collect.Sets;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;

import java.util.Set;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class LifecycleScope implements Scope {

    public static final LifecycleScope SCOPE = new LifecycleScope();

    private Set<Object> instances = Sets.newHashSet();

    @Override
    public <T> Provider<T> scope(Key<T> key, Provider<T> unscoped) {

        return new Provider<T>() {

            private volatile T instance = null;

            @Override
            public synchronized T get() {

                if (instance == null) {
                    instance = unscoped.get();
                    synchronized (instances){
                        instances.add(instance);
                    }
                }
                return instance;
            }
        };
    }

}

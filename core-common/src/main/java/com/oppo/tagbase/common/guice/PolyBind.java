package com.oppo.tagbase.common.guice;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.util.Types;
import com.oppo.tagbase.common.util.AnnotationUtil;

import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wujianchao on 2020/1/20.
 */
public final class PolyBind {

    @Deprecated
    public static <T> void bind(Binder binder, Class<T> i, String propKey, String defaultImpl) {
        binder.bind(i).toProvider(new PolyProvider<T>(i, propKey, defaultImpl)).in(Scopes.SINGLETON);
    }

    /**
     * 1. bind named implementation to interface by prop key
     */
    public static <T> void bind(Binder binder, Class<T> i) {
        Poly poly = AnnotationUtil.getAnnotation(i, Poly.class);
        binder.bind(i).toProvider(new PolyProvider<T>(i, poly.key(), poly.defaultImpl())).in(Scopes.SINGLETON);
    }

    /**
     * 2. register an implementation
     */
    public static <T> void registerImpl(Binder binder,
                                        Class<T> i,
                                        String implName,
                                        Class<? extends T> implClazz) {
        MapBinder.newMapBinder(binder, String.class, i)
                .addBinding(implName).to(implClazz).in(Scopes.SINGLETON);
    }


    static final class PolyProvider<T> implements Provider {

        private final String propKey;

        private final String defaultImpl;

        /**
         * interface class
         */
        private final Class<T> i;

        private Properties props;

        private Injector ij;


        private PolyProvider(Class<T> i, String propKey, String defaultImpl) {
            this.i = i;
            this.propKey = propKey;
            this.defaultImpl = defaultImpl;
        }

        @Inject
        void inject(Injector injector, Properties props) {
            this.ij = injector;
            this.props = props;
        }

        @Override
        public T get() {
            String implName = (String) props.get(propKey);
            if (implName == null) {
                implName = defaultImpl;
            }

            final Map<String, T> impls;
            final ParameterizedType mapType = Types.mapOf(String.class, i);

            impls = (Map<String, T>) ij.getInstance(Key.get(mapType));

            return impls.get(implName);
        }
    }

}

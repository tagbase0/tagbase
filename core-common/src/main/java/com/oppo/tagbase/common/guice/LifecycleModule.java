package com.oppo.tagbase.common.guice;

import com.google.inject.*;
import com.google.inject.name.Names;

import java.util.Set;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class LifecycleModule extends AbstractModule {

    @Override
    protected void configure() {
        binder().bind(Lifecycle.class).toProvider(new LifecycleProvider()).in(Scopes.SINGLETON);
        // register a handler which does nothing to avoid empty handler set.
        Lifecycle.registerInstance(binder(), Noop.class);
    }

    static class LifecycleProvider implements Provider<Lifecycle> {

        private Injector ij;

        @Inject
        private void inject(Injector ij) {
            this.ij = ij;
        }


        @Override
        public Lifecycle get() {
            final Key<Set<Class<?>>> key = Key.get(new TypeLiteral<Set<Class<?>>>() {
            }, Names.named("lifecycle"));
            final Set<Class<?>> classes = ij.getInstance(key);

            Lifecycle lifecycle = new Lifecycle();

            for (Class clazz : classes) {
                lifecycle.addManagedInstance(ij.getInstance(clazz));
            }

            return lifecycle;
        }
    }

    static class Noop {

        @LifecycleStart
        public void start() {

        }

        @LifecycleStop
        public void stop() {

        }
    }


}

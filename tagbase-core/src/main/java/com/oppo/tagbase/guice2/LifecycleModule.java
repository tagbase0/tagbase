package com.oppo.tagbase.guice2;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import java.util.Set;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class LifecycleModule extends AbstractModule {

    @Override
    protected void configure() {
        binder().bindScope(LifecycleBinding.class, LifecycleScope.SCOPE);
    }


    @Provides @Singleton
    public Lifecycle getLifecycle(final Injector injector){
        final Key<Set<Lifecycle.KeyHolder>> keyHolderKey = Key.get(new TypeLiteral<Set<Lifecycle.KeyHolder>>(){}, Names.named("lifecycle"));
        final Set<Lifecycle.KeyHolder> eagerClasses = injector.getInstance(keyHolderKey);

        Lifecycle lifecycle = new Lifecycle();
        for (Lifecycle.KeyHolder<?> holder : eagerClasses) {
            lifecycle.addManagedInstance(injector.getInstance(holder.getKey()));
        }

//        scope.setLifecycle(lifecycle);
//        lastScope.setLifecycle(lifecycle);

        return lifecycle;
    }
}

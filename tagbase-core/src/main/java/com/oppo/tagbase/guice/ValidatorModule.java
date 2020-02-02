package com.oppo.tagbase.guice;

import com.google.inject.AbstractModule;

import javax.validation.Validation;
import javax.validation.Validator;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class ValidatorModule extends AbstractModule {

    @Override
    protected void configure() {
        binder().bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
    }
}

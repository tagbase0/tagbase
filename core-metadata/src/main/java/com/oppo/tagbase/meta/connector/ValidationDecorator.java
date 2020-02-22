package com.oppo.tagbase.meta.connector;

import org.jdbi.v3.sqlobject.Handler;
import org.jdbi.v3.sqlobject.HandlerDecorator;

import java.lang.reflect.Method;

/**
 * Created by wujianchao on 2020/2/22.
 */
public class ValidationDecorator implements HandlerDecorator {
    @Override
    public Handler decorateHandler(Handler base, Class<?> sqlObjectType, Method method) {
        //TODO
        return null;
    }
}

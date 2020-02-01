package com.oppo.tagbase.guice2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class TestMain {

    public static void main(String[] args) {
        GuiceInjectors.makeInjector(
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule()
        );
    }
}

package com.oppo.tagbase.jobv2.skeleton;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class ExternalExecutable<C> {

    enum Name {
        BUILD_INVERTED_DICT,
        BUILD_BITMAP
        ;
    }

    private Name name;

    private C context;

    public ExternalExecutable(Name name, C context) {
        this.name = name;
        this.context = context;
    }

    public Name getName() {
        return name;
    }
}

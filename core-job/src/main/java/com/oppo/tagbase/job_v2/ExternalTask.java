package com.oppo.tagbase.job_v2;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class ExternalTask<C> {

    enum Name {
        BUILD_INVERTED_DICT,
        BUILD_BITMAP
        ;
    }

    private Name name;

    private C context;

    public ExternalTask(Name name, C context) {
        this.name = name;
        this.context = context;
    }

    public Name getName() {
        return name;
    }
}

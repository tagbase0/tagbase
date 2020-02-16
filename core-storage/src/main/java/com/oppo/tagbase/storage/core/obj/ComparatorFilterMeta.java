package com.oppo.tagbase.storage.core.obj;

import java.util.Comparator;

/**
 * Created by liangjingya on 2020/2/15.
 */
public class ComparatorFilterMeta implements Comparator<FilterMeta>{
    @Override
    public int compare(FilterMeta o1, FilterMeta o2) {
        return o1.getIndex() - o2.getIndex();
    }
}

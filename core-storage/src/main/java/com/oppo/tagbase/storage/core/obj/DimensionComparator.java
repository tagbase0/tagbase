package com.oppo.tagbase.storage.core.obj;

import java.util.Comparator;

/**
 * Created by liangjingya on 2020/2/15.
 */
public class DimensionComparator implements Comparator<DimensionContext>{

    @Override
    public int compare(DimensionContext o1, DimensionContext o2) {
        return o1.getDimIndex() - o2.getDimIndex();
    }

}

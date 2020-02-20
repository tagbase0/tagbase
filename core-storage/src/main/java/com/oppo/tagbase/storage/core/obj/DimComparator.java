package com.oppo.tagbase.storage.core.obj;

import java.util.Comparator;

/**
 * Created by liangjingya on 2020/2/15.
 */
public class DimComparator implements Comparator<DimContext>{

    @Override
    public int compare(DimContext o1, DimContext o2) {
        return o1.getDimIndex() - o2.getDimIndex();
    }

}

package com.oppo.tagbase.meta.util;

import org.junit.Test;

/**
 * Created by wujianchao on 2020/3/2.
 */
public class RangeUtilTest {

    @Test
    public void intersectionToSqlFilter() {
        String lowerField="dataLowerTime";
        String upperField="dataUpperTime";
        String queryLower="queryLower";
        String queryUpper="queryUpper";
        System.out.println(RangeUtil.intersectionToSqlFilter(lowerField, upperField, queryLower, queryUpper));
    }
}

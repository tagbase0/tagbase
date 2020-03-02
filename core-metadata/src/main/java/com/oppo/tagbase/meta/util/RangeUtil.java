package com.oppo.tagbase.meta.util;

import com.google.common.collect.Range;

import java.time.LocalDateTime;

/**
 * Created by wujianchao on 2020/2/17.
 */
public class RangeUtil {

    public static Range<LocalDateTime> of(LocalDateTime lower, LocalDateTime upper) {
        return Range.closedOpen(lower, upper);
    }

    public static LocalDateTime lowerEndpoint(Range<LocalDateTime> r) {
        return r.hasLowerBound()? r.lowerEndpoint() : LocalDateTime.MIN;
    }

    public static LocalDateTime upperEndpoint(Range<LocalDateTime> r) {
        return r.hasUpperBound() ? r.upperEndpoint() : LocalDateTime.MAX;
    }

    /**
     *
     * Let's assume the job timeline is like [2020-01-01,2020-01-03) [2020-01-03,2020-01-05) [2020-01-05,2020-01-07)
     *      1. query with dataLowerTime=2020-01-02 dataUpperTime=2020-01-05
     *      will return [2020-01-01,2020-01-03) [2020-01-03,2020-01-05)
     *      2. query with dataLowerTime=2020-01-04 dataUpperTime=2020-01-06
     *      will return [2020-01-03,2020-01-05) [2020-01-05,2020-01-07)
     */
    public static String intersectionToSqlFilter(String lowerField, String upperField, String queryLower, String queryUpper){
        return String.format(" !(%s<=:%s or %s>=:%s) ", upperField, queryLower, lowerField, queryUpper);
    }
}

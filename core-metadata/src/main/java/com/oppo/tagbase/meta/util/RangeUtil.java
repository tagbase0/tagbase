package com.oppo.tagbase.meta.util;

import com.google.common.collect.Range;

import java.time.LocalDateTime;

/**
 * Created by wujianchao on 2020/2/17.
 */
public class RangeUtil {

    public static LocalDateTime lowerEndpoint(Range<LocalDateTime> r) {
        return r.hasLowerBound()? r.lowerEndpoint() : LocalDateTime.MIN;
    }

    public static LocalDateTime upperEndpoint(Range<LocalDateTime> r) {
        return r.hasUpperBound() ? r.upperEndpoint() : LocalDateTime.MAX;
    }
}

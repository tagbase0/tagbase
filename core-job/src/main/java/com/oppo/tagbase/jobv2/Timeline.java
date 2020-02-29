package com.oppo.tagbase.jobv2;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.oppo.tagbase.meta.util.RangeUtil;

import java.time.LocalDateTime;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 * Created by wujianchao on 2020/2/26.
 */
public final class Timeline {

    private RangeSet<LocalDateTime> rangeSet = TreeRangeSet.create();
    private SortedSet<Range<LocalDateTime>> originRangeSet;

    public static Timeline of(SortedSet originRangeSet) {
        return new Timeline(originRangeSet);
    }

    private Timeline(SortedSet<Range<LocalDateTime>> originRangeSet) {
        for (Range<LocalDateTime> range : originRangeSet) {
            rangeSet.add(range);
        }
        this.originRangeSet = originRangeSet;
    }

    public boolean intersects(Range<LocalDateTime> range) {
        return rangeSet.intersects(range);
    }

    public boolean overlap(Range<LocalDateTime> range) {
        if (originRangeSet == null || originRangeSet.isEmpty()) {
            return false;
        }

        List<LocalDateTime> endpoints = originRangeSet.stream()
                .map(RangeUtil::lowerEndpoint)
                .collect(Collectors.toList());

        endpoints.add(RangeUtil.upperEndpoint(originRangeSet.last()));

        return endpoints.contains(RangeUtil.lowerEndpoint(range))
                && endpoints.contains(RangeUtil.upperEndpoint(range));
    }

    public boolean hasHole() {
        return rangeSet.asRanges().size() == 1;
    }

    public boolean encloses(Range<LocalDateTime> range) {
        return rangeSet.encloses(range);
    }

    public boolean isConnected(Range<LocalDateTime> range) {
        return rangeSet.span().isConnected(range);
    }

}

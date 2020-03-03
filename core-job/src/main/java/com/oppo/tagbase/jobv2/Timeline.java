package com.oppo.tagbase.jobv2;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.oppo.tagbase.meta.util.RangeUtil;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collector;
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

    public RangeSet<LocalDateTime> intersection(Range<LocalDateTime> range) {
        Set<Range<LocalDateTime>> ret = rangeSet.asRanges().stream()
                .map(r -> r.intersection(range))
                .filter(r -> !r.isEmpty())
                .collect(Collectors.toSet());
        return TreeRangeSet.create(ret);
    }

    /**
     * Weather timeline "overlap" a range.
     *
     * Timeline : [3,5) [5,7) [7,9) [20,30)
     *
     * case range :
     *      [1,2) => false
     *      [1,3) => false
     *      [3,5) => false
     *      [10,11) => false
     *
     *      [1,4) => true
     *      [3,4) => true
     *      [3,6) => true
     *      [4,6) => true
     *      [7, 10) => true
     */
    public boolean overlap(Range<LocalDateTime> range) {
        if (originRangeSet == null || originRangeSet.isEmpty()) {
            return false;
        }


        if(rangeSet.intersects(range)) {
            return false;
        }

        List<LocalDateTime> endpoints = originRangeSet.stream()
                .map(RangeUtil::lowerEndpoint)
                .collect(Collectors.toList());

        endpoints.add(RangeUtil.upperEndpoint(originRangeSet.last()));

        boolean ret = false;
        if(rangeSet.contains(RangeUtil.lowerEndpoint(range))) {
            ret = !endpoints.contains(RangeUtil.lowerEndpoint(range));
        }

        if(!ret){
            if(rangeSet.contains(RangeUtil.upperEndpoint(range))){
                ret = !endpoints.contains(RangeUtil.upperEndpoint(range));
            }
        }

        return ret;
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

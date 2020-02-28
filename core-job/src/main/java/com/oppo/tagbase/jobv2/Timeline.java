package com.oppo.tagbase.jobv2;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Set;

/**
 * Created by wujianchao on 2020/2/26.
 */
public final class Timeline {

    private RangeSet<LocalDateTime> rangeSet = TreeRangeSet.create();


    private Timeline(Set<Range<LocalDateTime>> ranges) {
        for (Range<LocalDateTime> range : ranges) {
            rangeSet.add(range);
        }
    }


    public boolean overlap(Range<LocalDateTime> range) {
//        range.
        return true;
    }


    class Builder {
        private Set<Range<LocalDateTime>> ranges = Sets.newHashSet();

        public Builder add(Collection<Range<LocalDateTime>> ranges) {
            this.ranges.addAll(ranges);
            return this;
        }

        public Timeline build() {
            return new Timeline(ranges);
        }
    }




}

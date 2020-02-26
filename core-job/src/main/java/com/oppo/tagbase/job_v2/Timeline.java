package com.oppo.tagbase.job_v2;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeSet;

import java.sql.Date;
import java.util.Collection;
import java.util.Set;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class Timeline {

    private RangeSet<Date> rangeSet = TreeRangeSet.create();


    private Timeline(Set<Range<Date>> ranges) {
        for (Range<Date> range : ranges) {
            rangeSet.add(range);
        }
    }


    public boolean overlap(Range<Date> range) {
        range.
    }


    class Builder {
        private Set<Range<Date>> ranges = Sets.newHashSet();

        public Builder add(Collection<Range<Date>> ranges) {
            this.ranges.addAll(ranges);
            return this;
        }

        public Timeline build() {
            return new Timeline(ranges);
        }
    }




}

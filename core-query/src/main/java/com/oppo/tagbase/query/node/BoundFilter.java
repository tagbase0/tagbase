package com.oppo.tagbase.query.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import javax.annotation.Nullable;


/**
 * Created by huangfeng on 2020/2/15.
 */
public class BoundFilter implements Filter {

    private final String column;
    @Nullable
    private final String upper;
    @Nullable
    private final String lower;
    private final boolean lowerStrict;
    private final boolean upperStrict;

    @JsonCreator
    public BoundFilter(
            @JsonProperty("column") String column,
            @JsonProperty("lower") @Nullable String lower,
            @JsonProperty("upper") @Nullable String upper,
            @JsonProperty("lowerStrict") @Nullable Boolean lowerStrict,
            @JsonProperty("upperStrict") @Nullable Boolean upperStrict) {
        this.column = Preconditions.checkNotNull(column, "column can not be null");
        Preconditions.checkState((lower != null) || (upper != null), "lower and upper can not be null at the same time");
        this.upper = upper;
        this.lower = lower;
        this.lowerStrict = (lowerStrict == null) ? false : lowerStrict;
        this.upperStrict = (upperStrict == null) ? false : upperStrict;

    }

    @Override
    public RangeSet<String> getDimensionRangeSet() {
        RangeSet<String> retSet = TreeRangeSet.create();
        Range<String> range;
        if (getLower() == null) {
            range = isUpperStrict() ? Range.lessThan(getUpper()) : Range.atMost(getUpper());
        } else if (getUpper() == null) {
            range = isLowerStrict() ? Range.greaterThan(getLower()) : Range.atLeast(getLower());
        } else {
            range = Range.range(getLower(), isLowerStrict() ? BoundType.OPEN : BoundType.CLOSED,
                    getUpper(), isUpperStrict() ? BoundType.OPEN : BoundType.CLOSED);
        }
        retSet.add(range);
        return retSet;
    }


    @Override
    public String getColumn() {
        return null;
    }

    @Override
    public boolean isExact() {
        return false;
    }

    @Nullable
    public String getUpper() {
        return upper;
    }

    @Nullable
    public String getLower() {
        return lower;
    }

    public boolean isLowerStrict() {
        return lowerStrict;
    }

    public boolean isUpperStrict() {
        return upperStrict;
    }
}

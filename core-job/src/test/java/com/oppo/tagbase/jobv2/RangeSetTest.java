package com.oppo.tagbase.jobv2;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.oppo.tagbase.meta.util.RangeUtil;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

/**
 * Created by wujianchao on 2020/2/27.
 */
public class RangeSetTest {

    LocalDateTime _0101 = LocalDateTime.of(2020, 01, 01, 0, 0);
    LocalDateTime _0105 = LocalDateTime.of(2020, 01, 05, 0, 0);
    LocalDateTime _0110 = LocalDateTime.of(2020, 01, 10, 0, 0);

    @Test
    public void rangeSizeTest(){
        RangeSet<LocalDateTime> rangeSet = TreeRangeSet.create();

        rangeSet.add(RangeUtil.of(_0101, _0105));
        rangeSet.add(RangeUtil.of(_0105, _0110));

        Assert.assertEquals(1, rangeSet.asRanges().size());

    }

    @Test
    public void rangeContainingTest() {
        RangeSet<LocalDateTime> rangeSet = TreeRangeSet.create();

        rangeSet.add(RangeUtil.of(_0101, _0105));
        rangeSet.add(RangeUtil.of(_0105, _0110));

        Range<LocalDateTime> r = rangeSet.rangeContaining(_0101);
        Assert.assertEquals(RangeUtil.of(_0101, _0110), r);
    }

    @Test
    public void intersectionTest() {
        RangeSet<LocalDateTime> rangeSet = TreeRangeSet.create();

        rangeSet.add(RangeUtil.of(_0101, _0105));
        rangeSet.add(RangeUtil.of(_0105, _0110));


        LocalDateTime _0120 = LocalDateTime.of(2020, 01, 20, 0, 0);;
        boolean intersects = rangeSet.intersects(RangeUtil.of(_0110, _0120));
        Assert.assertTrue(!intersects);
    }

    @Test
    public void singleValueTest() {
        LocalDateTime _0102 = LocalDateTime.of(2020, 01, 02, 0, 0);;
        Range<LocalDateTime> r1 = Range.singleton(_0101);
        Range<LocalDateTime> r2 = RangeUtil.of(_0101, _0102);

        Assert.assertNotEquals(r1, r2);

        Range<Integer> rr1 = Range.singleton(1);
        Range<Integer> rr2 = Range.closedOpen(1, 2);

        Assert.assertNotEquals(rr1, rr2);

    }
}

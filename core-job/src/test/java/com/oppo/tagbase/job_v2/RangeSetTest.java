package com.oppo.tagbase.job_v2;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.oppo.tagbase.meta.util.SqlDateUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;

/**
 * Created by wujianchao on 2020/2/27.
 */
public class RangeSetTest {

    Date _0101 = SqlDateUtil.create(2020, 00, 01);
    Date _0105 = SqlDateUtil.create(2020, 00, 05);
    Date _0110 = SqlDateUtil.create(2020, 00, 10);

    @Test
    public void rangeSizeTest(){
        RangeSet<Date> rangeSet = TreeRangeSet.create();

        rangeSet.add(Range.closedOpen(_0101, _0105));
        rangeSet.add(Range.closedOpen(_0105, _0110));

        Assert.assertEquals(1, rangeSet.asRanges().size());

    }

    @Test
    public void rangeContainingTest() {
        RangeSet<Date> rangeSet = TreeRangeSet.create();

        rangeSet.add(Range.closedOpen(_0101, _0105));
        rangeSet.add(Range.closedOpen(_0105, _0110));

        Range<Date> r = rangeSet.rangeContaining(_0101);
        Assert.assertEquals(Range.closedOpen(_0101, _0110), r);
    }

    @Test
    public void intersectionTest() {
        RangeSet<Date> rangeSet = TreeRangeSet.create();

        rangeSet.add(Range.closedOpen(_0101, _0105));
        rangeSet.add(Range.closedOpen(_0105, _0110));


        Date _0120 = SqlDateUtil.create(2020, 00, 20);
        boolean intersects = rangeSet.intersects(Range.closedOpen(_0110, _0120));
        Assert.assertTrue(!intersects);
    }

    @Test
    public void singleValueTest() {
        Date _0102 = SqlDateUtil.create(2020, 00, 02);
        Range<Date> r1 = Range.singleton(_0101);
        Range<Date> r2 = Range.closedOpen(_0101, _0102);

        Assert.assertNotEquals(r1, r2);

        Range<Integer> rr1 = Range.singleton(1);
        Range<Integer> rr2 = Range.closedOpen(1, 2);

        Assert.assertNotEquals(rr1, rr2);

    }
}

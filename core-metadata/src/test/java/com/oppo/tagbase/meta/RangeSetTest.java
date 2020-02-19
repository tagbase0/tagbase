package com.oppo.tagbase.meta;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.oppo.tagbase.meta.util.SqlDateUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;

/**
 * Created by wujianchao on 2020/2/19.
 */
public class RangeSetTest {

    @Test
    public void enclosedTest(){
        RangeSet<Date> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.closedOpen(SqlDateUtil.create(2020, 00, 01),
                SqlDateUtil.create(2020, 00, 10)));

        Range<Date> r = Range.closedOpen(SqlDateUtil.create(2020, 00, 01),
                SqlDateUtil.create(2020, 00, 10));

        Assert.assertTrue(rangeSet.encloses(r));
    }

    @Test
    public void enclosedWithCombinedRangeSetTest(){
        RangeSet<Date> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.closedOpen(SqlDateUtil.create(2020, 00, 01),
                SqlDateUtil.create(2020, 00, 5)));
        rangeSet.add(Range.closedOpen(SqlDateUtil.create(2020, 00, 5),
                SqlDateUtil.create(2020, 00, 10)));

        Range<Date> r = Range.closedOpen(SqlDateUtil.create(2020, 00, 01),
                SqlDateUtil.create(2020, 00, 10));

        Assert.assertTrue(rangeSet.encloses(r));
    }
}

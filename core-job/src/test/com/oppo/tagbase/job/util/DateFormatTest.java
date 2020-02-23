package com.oppo.tagbase.job.util;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Created by daikai on 2020/2/23.
 */
public class DateFormatTest {

    @Test
    public void toDate() {
        Assert.assertEquals(new DateFormat().toDate("2020-01-12 12:32").toString(), "2020-01-12");
    }
}
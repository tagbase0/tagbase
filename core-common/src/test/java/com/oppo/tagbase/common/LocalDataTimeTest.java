package com.oppo.tagbase.common;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

/**
 * Created by wujianchao on 2020/2/29.
 */
public class LocalDataTimeTest {

    @Test(expected = DateTimeParseException.class)
    public void parseTest() {
        System.out.println(LocalDateTime.parse("2020-02-02"));
    }
}

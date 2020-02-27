package com.oppo.tagbase.meta.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Created by wujianchao on 2020/2/17.
 */
@Deprecated
public class SqlDateUtil {

    public static Date now() {
        return new Date(System.currentTimeMillis());
    }

    public static Date addSomeDays(Date date, int nDays) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, nDays);
        return new Date(c.getTime().getTime());
    }

    public static Date create(int year, int month, int day) {
        Calendar.Builder builder = new Calendar.Builder()
                .setDate(year, month, day)
                .setTimeOfDay(0, 0, 0, 0);
        return new Date(builder.build().getTime().getTime());

    }

    public static void main(String[] args) {
        Timestamp t = new Timestamp(System.currentTimeMillis());

    }
}

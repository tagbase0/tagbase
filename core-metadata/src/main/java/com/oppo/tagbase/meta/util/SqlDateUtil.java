package com.oppo.tagbase.meta.util;

import java.sql.Date;
import java.util.Calendar;

/**
 * Created by wujianchao on 2020/2/17.
 */
public class SqlDateUtil {

    public static Date addSomeDates(Date date, int nDays) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, nDays);
        return new Date(c.getTime().getTime());
    }
}

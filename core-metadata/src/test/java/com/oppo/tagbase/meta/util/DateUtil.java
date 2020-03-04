package com.oppo.tagbase.meta.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by daikai on 2020/3/4.
 */
public class DateUtil {

    static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static LocalDateTime toLocalDateTime(String date){

        return LocalDateTime.parse(date, DATE_TIME_FORMATTER);
    }
}

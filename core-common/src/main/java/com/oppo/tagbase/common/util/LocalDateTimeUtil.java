package com.oppo.tagbase.common.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Created by wujianchao on 2020/2/27.
 */
public class LocalDateTimeUtil {

    public static LocalDateTime of(long milliseconds) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(milliseconds), ZoneId.systemDefault());
    }

    public static long toEpochMillis(LocalDateTime dateTime) {
        return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    public static long minus(LocalDateTime left, LocalDateTime right) {
        return toEpochMillis(left) - toEpochMillis(right);
    }
}

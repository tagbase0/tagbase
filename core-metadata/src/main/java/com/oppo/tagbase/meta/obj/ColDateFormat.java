package com.oppo.tagbase.meta.obj;

/**
 * Created by wujianchao on 2020/3/6.
 */
public enum ColDateFormat {

    ISO_DATE("yyyy-MM-dd"),
    ISO_HOUR("yyyy-MM-ddTHH"),
    ISO_DATE_TIME("yyyy-MM-ddTHH-mm-ss"),

    HIVE_DATE("yyyyMMdd"),
    HIVE_HOUR("yyyyMMddHH"),
    HIVE_DATE_TIME("yyyyMMddHHmmss"),
    ;

    String format;
    ColDateFormat(String format) {
        this.format = format;
    }

    public String getFormat() {
        return format;
    }
}

package com.oppo.tagbase.meta;

import com.oppo.tagbase.common.ErrorCode;

/**
 * Created by wujianchao on 2020/2/22.
 */
public enum MetadataErrorCode implements ErrorCode {

    DB_NOT_EXIST(100, "db not exist"),
    TABLE_NOT_EXIST(100, "table not exist"),

    DUPLICATE_DB_NAME(100, "duplicate db name"),
    DUPLICATE_TABLE_NAME(100, "duplicate table name");


    private int code;
    private String reason;
    private Family family = Family.METADATA;

    MetadataErrorCode(int code, String reason) {
        this.code = code;
        this.reason = reason;
    }

    @Override
    public int getErrorCode() {
        return code;
    }

    @Override
    public Family getFamily() {
        return family;
    }

    @Override
    public String getReason() {
        return reason;
    }

}

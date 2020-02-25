package com.oppo.tagbase.meta;

import com.oppo.tagbase.common.ErrorCode;

/**
 * Created by wujianchao on 2020/2/22.
 */
public enum MetadataErrorCode implements ErrorCode {

    DB_NOT_EXIST(100),
    TABLE_NOT_EXIST(101),

    DUPLICATE_DB_NAME(102),
    DUPLICATE_TABLE_NAME(103),

    METADATA_ERROR(199)
    ;


    private int code;
    private String name;
    private Family family = Family.METADATA;

    MetadataErrorCode(int code) {
        this.name = name();
        this.code = code;
    }

    @Override
    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    @Override
    public Family getFamily() {
        return family;
    }

}

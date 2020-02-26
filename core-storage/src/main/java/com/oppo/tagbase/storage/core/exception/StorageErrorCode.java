package com.oppo.tagbase.storage.core.exception;

import com.oppo.tagbase.common.ErrorCode;

/**
 * Created by liangjingya on 2020/2/26.
 */
public enum StorageErrorCode implements ErrorCode {

    INIT_STORAGE_ERROR(300),
    STORAGE_INSERT_ERROR(301),
    STORAGE_QUERY_ERROR(302),
    STORAGE_TABLE_ERROR(303),
    DESER_BITMAP_ERROR(304),
    STORAGE_OTHER_ERROR(399);

    private int code;

    private String name;

    private Family family = Family.STORAGE;

    StorageErrorCode(int code) {
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

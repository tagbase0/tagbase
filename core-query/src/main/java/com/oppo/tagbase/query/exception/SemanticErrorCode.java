package com.oppo.tagbase.query.exception;

import com.oppo.tagbase.common.ErrorCode;

/**
 * @author huangfeng
 * @date 2020/2/21 8:07
 */
public enum SemanticErrorCode implements ErrorCode {

    MISSING_DB(501),
    MISSING_TABLE(502),
    MISSING_COLUMN(503),

    DUPLICATE_FILTER_COLUMN(503),

    SLICE_MUST_BE_BOUND_FILTER(504),
    NOT_SUPPORTED(505),
    WRONG_DATE_FORMAT(506),
    ;
    private int code;

    private String name;

    private Family family = Family.QUERY;

    SemanticErrorCode(int code) {
        this.code = code;
        this.name = name();
    }


    @Override
    public int getCode() {
        return code;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Family getFamily() {
        return family;
    }
}

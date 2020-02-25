package com.oppo.tagbase.query.exception;

/**
 * @author huangfeng
 * @date 2020/2/21 8:07
 */
public enum SemanticErrorCode implements ErrorCodeSupplier {

    MISSING_DB(501),
    MISSING_TABLE(502),
    MISSING_COLUMN(503),

    DUPLICATE_FILTER_COLUMN(503),

    SLICE_MUST_BE_BOUND_FILTER(504),
    NOT_SUPPORTED(505),
    WRONG_DATE_FORMAT(506),
    ;
    private ErrorCode errorCode;

    SemanticErrorCode(int code) {
        errorCode = new ErrorCode(code, name());
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}

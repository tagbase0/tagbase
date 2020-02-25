package com.oppo.tagbase.query.exception;

/**
 * @author huangfeng
 * @date 2020/2/22 19:27
 */
public enum QueryErrorCode implements ErrorCodeSupplier {
    QUERY_CANCELLED(521),
    QUERY_NOT_COMPLETE(522),
    QUERY_RUNNING_ERROR(523),
    QUERY_NOT_EXIST(524);
    private ErrorCode errorCode;

    QueryErrorCode(int code) {
        errorCode = new ErrorCode(code, name());
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}

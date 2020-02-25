package com.oppo.tagbase.query.exception;

/**
 * @author huangfeng
 * @date 2020/2/22 19:29
 */
public class QueryException extends ErrorCodeException {

    public QueryException(ErrorCodeSupplier errorCodeSupplier, String format, Object... args) {
        super(errorCodeSupplier, format, args);
    }

    public QueryException(ErrorCodeSupplier errorCodeSupplier, String message) {
        super(errorCodeSupplier, message);
    }


    public QueryException(ErrorCodeSupplier errorCode,Throwable cause) {
        super(cause, errorCode);
    }
}

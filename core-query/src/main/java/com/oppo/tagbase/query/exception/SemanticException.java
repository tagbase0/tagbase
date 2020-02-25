package com.oppo.tagbase.query.exception;

/**
 * @author huangfeng
 * @date 2020/2/21 8:04
 */
public class SemanticException extends ErrorCodeException {


    public SemanticException(ErrorCodeSupplier errorCodeSupplier, String format, Object... args) {
        super(errorCodeSupplier, format, args);
    }

    public SemanticException(ErrorCodeSupplier errorCodeSupplier, String message) {
        super(errorCodeSupplier, message);
    }

    @Override
    public String getMessage() {
        return super.getMessage();
    }
}

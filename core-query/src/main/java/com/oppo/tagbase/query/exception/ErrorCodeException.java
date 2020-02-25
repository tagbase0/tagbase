package com.oppo.tagbase.query.exception;

import static java.lang.String.format;

/**
 * @author huangfeng
 * @date 2020/2/24 15:01
 */
public abstract class ErrorCodeException extends RuntimeException {
    ErrorCode errorCode;


    public ErrorCodeException(ErrorCodeSupplier errorCodeSupplier, String format, Object... args) {
        super(formatMessage(format, args));

        this.errorCode = errorCodeSupplier.toErrorCode();
    }


    public ErrorCodeException(ErrorCodeSupplier errorCodeSupplier, String message) {
        super(message);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    public ErrorCodeException(Throwable cause, ErrorCodeSupplier errorCodeSupplier) {
        super(cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    public int getErrorCode() {
        return errorCode.getCode();
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        if (message == null) {
            message = errorCode.getName();
        }
        return message;
    }

    private static String formatMessage(String formatString, Object[] args) {
        return format(formatString, args);
    }


}

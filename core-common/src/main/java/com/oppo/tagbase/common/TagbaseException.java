package com.oppo.tagbase.common;

/**
 * Created by wujianchao on 2020/2/24.
 */
public abstract class TagbaseException extends RuntimeException {

    private ErrorCode errorCode;

    public TagbaseException(ErrorCode errorCode, String reason) {
        super(getReason(errorCode, reason));
        this.errorCode = errorCode;
    }

    public TagbaseException(ErrorCode errorCode, String reasonFormat, Object... args) {
        super(getReason(errorCode, reasonFormat, args));
        this.errorCode = errorCode;
    }

    public TagbaseException(ErrorCode errorCode, Throwable cause) {
        super(getReason(errorCode), cause);
        this.errorCode = errorCode;
    }

    public TagbaseException(ErrorCode errorCode, Throwable cause, String reason) {
        super(getReason(errorCode, reason), cause);
        this.errorCode = errorCode;
    }

    public TagbaseException(ErrorCode errorCode, Throwable cause, String reasonFormat, Object... args) {
        super(getReason(errorCode, reasonFormat, args), cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }


    protected static String getReason(ErrorCode errorCode) {
        return errorCode.format();
    }

    protected static String getReason(ErrorCode errorCode, String reason) {
        return errorCode.format() + " " + reason;
    }

    protected static String getReason(ErrorCode errorCode, String format, Object... args) {
        return errorCode.format() + " " + String.format(format, args);
    }


}

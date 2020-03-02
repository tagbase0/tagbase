package com.oppo.tagbase.query.exception;

import com.oppo.tagbase.common.ErrorCode;
import com.oppo.tagbase.common.TagbaseException;

/**
 * @author huangfeng
 * @date 2020/2/21 8:04
 */
public class SemanticException extends TagbaseException {


    public SemanticException(ErrorCode errorCode, String reason) {
        super(errorCode, reason);
    }

    public SemanticException(ErrorCode errorCode, String reasonFormat, Object... args) {
        super(errorCode, reasonFormat, args);
    }

    public SemanticException(ErrorCode errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public SemanticException(ErrorCode errorCode, Throwable cause, String reason) {
        super(errorCode, cause, reason);
    }

    public SemanticException(ErrorCode errorCode, Throwable cause, String reasonFormat, Object... args) {
        super(errorCode, cause, reasonFormat, args);
    }
}

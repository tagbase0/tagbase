package com.oppo.tagbase.query.exception;

import com.oppo.tagbase.common.ErrorCode;
import com.oppo.tagbase.common.TagbaseException;

/**
 * @author huangfeng
 * @date 2020/2/22 19:29
 */
public class QueryException extends TagbaseException {


    public QueryException(ErrorCode errorCode, String reason) {
        super(errorCode, reason);
    }

    public QueryException(ErrorCode errorCode, String reasonFormat, Object... args) {
        super(errorCode, reasonFormat, args);
    }

    public QueryException(ErrorCode errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public QueryException(ErrorCode errorCode, Throwable cause, String reason) {
        super(errorCode, cause, reason);
    }

    public QueryException(ErrorCode errorCode, Throwable cause, String reasonFormat, Object... args) {
        super(errorCode, cause, reasonFormat, args);
    }
}

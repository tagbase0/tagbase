package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.common.ErrorCode;
import com.oppo.tagbase.common.TagbaseException;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobException extends TagbaseException {

    public JobException(String reason) {
        this(JobErrorCode.JOB_ERROR, reason);
    }

    public JobException(String reasonFormat, Object... args) {
        this(JobErrorCode.JOB_ERROR, reasonFormat, args);
    }

    public JobException(Throwable cause, String reasonFormat, Object... args) {
        this(JobErrorCode.JOB_ERROR, cause, reasonFormat, args);
    }

    public JobException(ErrorCode errorCode, String reason) {
        super(errorCode, reason);
    }

    public JobException(ErrorCode errorCode, String reasonFormat, Object... args) {
        super(errorCode, reasonFormat, args);
    }

    public JobException(ErrorCode errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public JobException(ErrorCode errorCode, Throwable cause, String reason) {
        super(errorCode, cause, reason);
    }

    public JobException(ErrorCode errorCode, Throwable cause, String reasonFormat, Object... args) {
        super(errorCode, cause, reasonFormat, args);
    }

}

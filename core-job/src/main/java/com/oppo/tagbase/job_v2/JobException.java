package com.oppo.tagbase.job_v2;

import com.oppo.tagbase.common.ErrorCode;
import com.oppo.tagbase.common.TagbaseException;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobException extends TagbaseException {

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

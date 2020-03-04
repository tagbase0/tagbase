package com.oppo.tagbase.jobv2;

import static com.oppo.tagbase.jobv2.JobErrorCode.JOB_STATE_TRANSFER;

/**
 * Created by wujianchao on 2020/3/4.
 */
public class JobStateException extends JobException {

    public JobStateException(String reason) {
        super(JOB_STATE_TRANSFER, reason);
    }

    public JobStateException(String reasonFormat, Object... args) {
        super(JOB_STATE_TRANSFER, reasonFormat, args);
    }

    public JobStateException(Throwable cause, String reasonFormat, Object... args) {
        super(JOB_STATE_TRANSFER, cause, reasonFormat, args);
    }

}

package com.oppo.tagbase.job.engine.exception;

import com.oppo.tagbase.common.ErrorCode;

/**
 * Created by liangjingya on 2020/2/26.
 */
public enum JobErrorCode implements ErrorCode {

    JOB_SUBMIT_ERROR(500),
    JOB_MONITOR_ERROR(501),
    JOB_OTHER_ERROR(599);

    private int code;

    private String name;

    private Family family = Family.STORAGE;

    JobErrorCode(int code) {
        this.name = name();
        this.code = code;
    }

    @Override
    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    @Override
    public Family getFamily() {
        return family;
    }

}

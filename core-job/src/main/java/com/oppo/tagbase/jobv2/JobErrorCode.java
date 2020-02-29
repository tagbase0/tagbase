package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.common.ErrorCode;

/**
 * Created by wujianchao on 2020/2/26.
 */
public enum JobErrorCode implements ErrorCode {


    DICT_HAS_HOLE(501),
    TIME_OVER_FLOW(502),
    JOB_OVERLAP(503),
    SLICE_OVERLAP(504),
    JOB_ERROR(500)
    ;

    private int code;
    private String name;
    private Family family = Family.JOB;

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

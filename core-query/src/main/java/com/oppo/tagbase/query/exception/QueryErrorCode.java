package com.oppo.tagbase.query.exception;

/**
 * @author huangfeng
 * @date 2020/2/22 19:27
 */
public enum QueryErrorCode implements com.oppo.tagbase.common.ErrorCode {
    QUERY_CANCELLED(521),
    QUERY_NOT_COMPLETE(522),
    QUERY_RUNNING_ERROR(523),
    QUERY_NOT_EXIST(524);

    private int code;

    private String name;

    private Family family = Family.QUERY;


    QueryErrorCode(int code) {
        this.code = code;
        this.name = name();
    }


    @Override
    public int getCode() {
        return code;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Family getFamily() {
        return family;
    }
}

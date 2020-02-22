package com.oppo.tagbase.common;

/**
 * Created by wujianchao on 2020/2/22.
 */
public enum TestErrorCode implements ErrorCode {

    TEST_ERROR(600, "test");

    private int code;
    private Family family;
    private String reason;

    TestErrorCode(int code, String reason) {
        this.code = code;
        this.reason = reason;
    }

    @Override
    public int getErrorCode() {
        return code;
    }

    @Override
    public Family getFamily() {
        switch (code / 100) {
            case 1:
                this.family = Family.METADATA;
                break;
            case 2:
                this.family = Family.DICTIONARY;
                break;
            case 3:
                this.family = Family.STORAGE;
                break;
            case 4:
                this.family = Family.QUERY;
                break;
            case 5:
                this.family = Family.JOB;
                break;
            default:
                this.family = Family.OTHER;
                break;
        }
        return family;
    }

    @Override
    public String getReason() {
        return reason;
    }

}

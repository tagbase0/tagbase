package com.oppo.tagbase.common;

/**
 * Created by wujianchao on 2020/2/22.
 */
public enum TestErrorCode implements ErrorCode {

    TEST_ERROR(600, "test");

    private int code;
    private String name;
    private Family family;

    TestErrorCode(int code, String name) {
        this.code = code;
        this.name = name;
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


}

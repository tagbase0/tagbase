package com.oppo.tagbase.meta.connector;

/**
 * Created by wujianchao on 2020/2/10.
 */
public class MCE extends RuntimeException{

    public MCE() {
        super();
    }

    public MCE(String message) {
        super(message);
    }

    public MCE(String message, Throwable cause) {
        super(message, cause);
    }
}

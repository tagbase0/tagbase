package com.oppo.tagbase.dict;

/**
 * Created by wujianchao on 2020/2/20.
 */
public class DictionaryException extends RuntimeException {

    public DictionaryException() {
        super();
    }

    public DictionaryException(String message) {
        super(message);
    }

    public DictionaryException(String message, Throwable cause) {
        super(message, cause);
    }
}

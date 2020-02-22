package com.oppo.tagbase.storage.core.exception;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class StorageException extends Exception {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause){
        super(message, cause);
    }

}

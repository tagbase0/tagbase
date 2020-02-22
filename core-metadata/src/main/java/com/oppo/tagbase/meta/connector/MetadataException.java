package com.oppo.tagbase.meta.connector;

/**
 * Created by wujianchao on 2020/2/10.
 */
public class MetadataException extends RuntimeException{

    public MetadataException() {
        super();
    }

    public MetadataException(String message) {
        super(message);
    }

    public MetadataException(String message, Throwable cause) {
        super(message, cause);
    }
}

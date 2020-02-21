package com.oppo.tagbase.job.exception;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class JobException extends Exception{

    public JobException(String message) {
        super(message);
    }

    public JobException(String message, Throwable cause){
        super(message, cause);
    }

}

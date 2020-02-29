package com.oppo.tagbase.jobv2.spi;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class TaskStatus {

    private boolean isDone;
    private boolean isSuccess;
    private String errorMessage;

    public boolean isDone() {
        return isDone;
    }

    public void setDone(boolean done) {
        isDone = done;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}

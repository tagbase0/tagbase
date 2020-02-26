package com.oppo.tagbase.common;

import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/22.
 */
@Deprecated
public final class ResourceResponse<T> {

    public static final int CODE_OK = 0;

    private int code;
    private String message;
    private T data;

    private ResourceResponse(int code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public static <T> ResourceResponse ok(T data){
        return new ResourceResponse(CODE_OK, "", data);
    }

    public static <T> ResourceResponse error(TagbaseException error){
        return new ResourceResponse(error.getErrorCode().getCode(), error.getMessage(), null);
    }


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceResponse<?> that = (ResourceResponse<?>) o;
        return code == that.code &&
                Objects.equals(message, that.message) &&
                Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, message, data);
    }
}

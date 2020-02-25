package com.oppo.tagbase.query.exception;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author huangfeng
 * @date 2020/2/24 15:02
 */
public final class ErrorCode {
    private final int code;
    private final String name;

    @JsonCreator
    public ErrorCode(
            @JsonProperty("code") int code,
            @JsonProperty("name") String name
            ) {
        if (code < 0) {
            throw new IllegalArgumentException("code is negative");
        }
        this.code = code;
        this.name = requireNonNull(name, "name is null");
    }

    @JsonProperty
    public int getCode() {
        return code;
    }

    @JsonProperty
    public String getName() {
        return name;
    }


    @Override
    public String toString() {
        return name + ":" + code;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ErrorCode that = (ErrorCode) obj;
        return Objects.equals(this.code, that.code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code);
    }
}
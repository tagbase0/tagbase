package com.oppo.tagbase.query.exception;

import static java.lang.String.format;

/**
 * @author huangfeng
 * @date 2020/2/21 8:04
 */
public class SemanticException extends RuntimeException {
    SemanticErrorCode code;

    public SemanticException(SemanticErrorCode code, String format, Object... args) {
        super(formatMessage(format, args));
        this.code = code;
    }



    private static String formatMessage(String formatString, Object[] args) {
        return format(formatString, args);
    }
}

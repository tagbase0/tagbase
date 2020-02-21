package com.oppo.tagbase.query.exception;

/**
 * @author huangfeng
 * @date 2020/2/21 8:07
 */
public enum SemanticErrorCode {

    MISSING_DB,
    MISSING_TABLE,
    MISSING_COLUMN,

    DUPLICATE_FILTER_COLUMN,

    SLICE_MUST_BE_BOUND_FILTER,
    NOT_SUPPORTED,
    WRONG_DATE_FORMAT
}

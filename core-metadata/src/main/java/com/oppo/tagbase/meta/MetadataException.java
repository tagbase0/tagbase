package com.oppo.tagbase.meta;

import com.oppo.tagbase.common.ErrorCode;
import com.oppo.tagbase.common.TagbaseException;

/**
 * Created by wujianchao on 2020/2/10.
 */
public class MetadataException extends TagbaseException {

    public MetadataException(ErrorCode errorCode, String reason) {
        super(errorCode, reason);
    }

    public MetadataException(ErrorCode errorCode, String reasonFormat, Object... args) {
        super(errorCode, reasonFormat, args);
    }

    public MetadataException(ErrorCode errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public MetadataException(ErrorCode errorCode, Throwable cause, String reason) {
        super(errorCode, cause, reason);
    }

    public MetadataException(ErrorCode errorCode, Throwable cause, String reasonFormat, Object... args) {
        super(errorCode, cause, reasonFormat, args);
    }
}

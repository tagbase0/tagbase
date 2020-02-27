package com.oppo.tagbase.storage.core.exception;

import com.oppo.tagbase.common.ErrorCode;
import com.oppo.tagbase.common.TagbaseException;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class StorageException extends TagbaseException {

    public StorageException(ErrorCode errorCode, String reason) {
        super(errorCode, reason);
    }

    public StorageException(ErrorCode errorCode, String reasonFormat, Object... args) {
        super(errorCode, reasonFormat, args);
    }

    public StorageException(ErrorCode errorCode, Throwable cause) {
        super(errorCode, cause);
    }

    public StorageException(ErrorCode errorCode, Throwable cause, String reason) {
        super(errorCode, cause, reason);
    }

    public StorageException(ErrorCode errorCode, Throwable cause, String reasonFormat, Object... args) {
        super(errorCode, cause, reasonFormat, args);
    }

}

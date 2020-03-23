package com.oppo.tagbase.dict.util;

import com.oppo.tagbase.dict.DictionaryException;

import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/22.
 */
public class Preconditions {

    public static void check(boolean expression, String message) {
        if(expression) {
            throw new DictionaryException(message);
        }
    }

    public static void checkNotEquals(Object left, Object right, String message) {
        if(!Objects.equals(left, right)) {
            throw new DictionaryException(message);
        }
    }
}

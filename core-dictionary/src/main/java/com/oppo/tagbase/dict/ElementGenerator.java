package com.oppo.tagbase.dict;

import com.oppo.tagbase.common.util.BytesUtil;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by wujianchao on 2020/2/21.
 */
public class ElementGenerator {

    // a-z, 0-9, except: l, o, 0, 1
    private static final char[] BASE_32 = {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '2', '3', '4', '5', '6', '7', '8', '9'};

    public static byte[] generate(int length) {
        StringBuilder element = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            element.append(BASE_32[ThreadLocalRandom.current().nextInt(32)]);
        }
        return BytesUtil.toUTF8Bytes(element.toString());
    }
}

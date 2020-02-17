package com.oppo.tagbase.dict.util;

/**
 * Created by wujianchao on 2020/2/14.
 */
public class UnsignedTypes {

    public static int unsignedByte(byte v) {
        return v & 0xFF;
    }

    public static int unsignedShort(short v) {
        return v & 0xFFFF;
    }

    public static long unsignedInt(int v) {
        return v & 0xFFFFFFFFL;
    }
}

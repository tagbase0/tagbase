package com.oppo.tagbase.common.util;

/**
 * Created by wujianchao on 2020/2/14.
 */
public class UnsignedTypes {

    public static int unsignedByte(byte x) {
        return ((int) x) & 0xff;
    }

    public static int unsignedShort(short x) {
        return ((int) x) & 0xffff;
    }

    public static long unsignedInt(int x) {
        return ((long) x) & 0xffffffffL;
    }
}

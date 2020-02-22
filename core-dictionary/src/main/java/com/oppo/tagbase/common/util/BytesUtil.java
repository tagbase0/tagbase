package com.oppo.tagbase.common.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Created by wujianchao on 2020/2/11.
 */
public class BytesUtil {

    public static void writeUTFString(ByteBuffer out, String s) {
        byte[] bytes = s == null ? null : s.getBytes(StandardCharsets.UTF_8);
        writeByteArray(out, bytes);
    }

    public static void writeByteArray(ByteBuffer out, byte[] array) {
        if (array == null) {
            writeInt(out, -1);
            return;
        }
        writeInt(out, array.length);
        out.put(array);
    }

    public static void writeByte(ByteBuffer out, byte value) {
        out.put(value);
    }

    public static void writeInt(ByteBuffer out, short value) {
        out.putShort(value);
    }

    public static void writeInt(ByteBuffer out, int value) {
        out.putInt(value);
    }

    public static void writeLong(ByteBuffer out, long value) {
        out.putLong(value);
    }

    public static boolean isNull(byte[] value) {
        return value == null || value.length == 0;
    }


    public static String toUTF8String(byte[] x) {
        return new String(x, StandardCharsets.UTF_8);
    }

    public static byte[] toUTF8Bytes(String x) {
        return x.getBytes(StandardCharsets.UTF_8);
    }


    public static String toUTF8String(ByteBuffer buf, int length) {
        if(length < 0) {
            throw new RuntimeException("length must be positive");
        }
        byte[] someBytes = new byte[length];
        buf.get(someBytes);
        return toUTF8String(someBytes);
    }

}

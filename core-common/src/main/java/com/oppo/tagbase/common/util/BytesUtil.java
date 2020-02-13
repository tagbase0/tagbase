package com.oppo.tagbase.common.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Created by wujianchao on 2020/2/11.
 */
public class BytesUtil {

    public static void writeUTFString(ByteBuffer out, String s) {
        byte[] bytes = s == null ? null : s.getBytes(Charset.forName("UTF-8"));
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


}

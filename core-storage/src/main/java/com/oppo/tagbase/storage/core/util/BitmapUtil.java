package com.oppo.tagbase.storage.core.util;

import com.oppo.tagbase.storage.core.connector.StorageException;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class BitmapUtil {

    public static byte[] serializeBitmap(ImmutableRoaringBitmap bitmap) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        bitmap.serialize(dos);
        dos.close();
        return bos.toByteArray();
    }

    public static ImmutableRoaringBitmap deSerializeBitmap(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        return new ImmutableRoaringBitmap(buffer);
    }

    public static ImmutableRoaringBitmap deSerializeBitmap(byte[] value, int offset, int length) {
        if (offset < 0 || length > value.length) {
            throw new StorageException("deSerializeBitmap error, illegal parameter");
        }
        ByteBuffer buffer = ByteBuffer.wrap(value, offset, length);
        return new ImmutableRoaringBitmap(buffer);
    }

}

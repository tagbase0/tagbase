package com.oppo.tagbase.storage.core.example.testobj;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

/**
 * @author huangfeng
 * @date 2020/2/7
 */

public class TagBitmap {

    public static final TagBitmap EOF = new TagBitmap(null,null);

    byte[][] key;
    ImmutableRoaringBitmap bitmap;

    public TagBitmap(byte[][] key, ImmutableRoaringBitmap bitmap) {
        this.key = key;
        this.bitmap = bitmap;
    }

    public byte[][] getKey() {
        return key;
    }

    public void setKey(byte[][] key) {
        this.key = key;
    }

    public ImmutableRoaringBitmap getBitmap() {
        return bitmap;
    }

    public void setBitmap(ImmutableRoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if(key == null){
            builder.append("null,");
        }else {
            for (byte[] value : key) {
                builder.append(new String(value) + ",");
            }
        }
        return "TagBitmap{" +
                "key=" + builder.toString() +
                " bitmap=" + bitmap +
                '}';
    }

}

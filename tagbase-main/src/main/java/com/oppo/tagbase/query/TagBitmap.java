package com.oppo.tagbase.query;

import org.roaringbitmap.RoaringBitmap;

/**
 * @author huangfeng
 * @date 2020/2/7
 */

public class TagBitmap {

    public static final TagBitmap EOF = new TagBitmap();


    String key;
    RoaringBitmap bitmap;


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public RoaringBitmap getBitmap() {
        return bitmap;
    }

    public void setBitmap(RoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }
}

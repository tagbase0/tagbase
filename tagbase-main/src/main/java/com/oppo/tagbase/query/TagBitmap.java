package com.oppo.tagbase.query;

import org.roaringbitmap.RoaringBitmap;


public class TagBitmap {
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

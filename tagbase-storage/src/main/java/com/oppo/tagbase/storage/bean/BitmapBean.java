package com.oppo.tagbase.storage.bean;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

/**
 * Created by liangjingya on 2020/2/10.
 */
public class BitmapBean {

    private String dimensionValue;

    private ImmutableRoaringBitmap bitmap;

    public BitmapBean(String dimensionValue, ImmutableRoaringBitmap bitmap) {
        this.dimensionValue = dimensionValue;
        this.bitmap = bitmap;
    }

    public String getDimensionValue() {
        return dimensionValue;
    }

    public void setDimensionValue(String dimensionValue) {
        this.dimensionValue = dimensionValue;
    }

    public ImmutableRoaringBitmap getBitmap() {
        return bitmap;
    }

    public void setBitmap(ImmutableRoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    @Override
    public String toString() {
        return "bitmapBean{" +
                "dimensionValue='" + dimensionValue + '\'' +
                ", bitmap=" + bitmap +
                '}';
    }
}

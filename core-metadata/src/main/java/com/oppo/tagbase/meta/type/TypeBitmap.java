package com.oppo.tagbase.meta.type;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Created by wujianchao on 2020/2/19.
 */
public class TypeBitmap implements DataTypeV2 {

    public static TypeBitmap TYPE = new TypeBitmap();

    @Override
    public Class<?> javaType() {
        return ImmutableRoaringBitmap.class;
    }
}

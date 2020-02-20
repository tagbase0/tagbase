package com.oppo.tagbase.meta.type;

/**
 * Created by wujianchao on 2020/2/19.
 */
public class TypeBigInt implements DataTypeV2 {

    public static TypeBigInt TYPE = new TypeBigInt();

    @Override
    public Class<?> javaType() {
        return Long.class;
    }

}

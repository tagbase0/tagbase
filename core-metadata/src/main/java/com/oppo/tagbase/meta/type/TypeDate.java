package com.oppo.tagbase.meta.type;

import java.sql.Date;

/**
 * Created by wujianchao on 2020/2/19.
 */
public class TypeDate implements DataTypeV2 {

    public static TypeDate TYPE = new TypeDate();

    @Override
    public Class<?> javaType() {
        return Date.class;
    }

}

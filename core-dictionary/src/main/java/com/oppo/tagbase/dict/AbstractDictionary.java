package com.oppo.tagbase.dict;

import java.nio.ByteBuffer;

/**
 * Created by wujianchao on 2020/2/11.
 */
public abstract class AbstractDictionary implements Dictionary {

    protected ByteBuffer data;

    @Override
    public long elementNum() {
        return 0;
    }

    @Override
    public byte[] element(long id) {
        return null;
    }

    @Override
    public long id(byte[] element) {
        return 0;
    }

}

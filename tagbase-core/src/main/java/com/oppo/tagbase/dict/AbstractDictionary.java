package com.oppo.tagbase.dict;

import java.nio.ByteBuffer;

/**
 * Created by wujianchao on 2020/2/11.
 */
public abstract class AbstractDictionary implements AppendableDictionary {

    protected ByteBuffer data;

    @Override
    public int nextId() {
        return 0;
    }

    @Override
    public void add(byte[] v) {

    }

    @Override
    public byte[] value(int id) {
        return null;
    }

    @Override
    public int id(byte[] v) {
        return 0;
    }

}

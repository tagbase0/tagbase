package com.oppo.tagbase.dict;

/**
 * Created by wujianchao on 2020/2/11.
 */
public class InvertedDictionary extends AbstractDictionary {


    @Override
    public byte[] element(int id) {
        throw new UnsupportedOperationException("Inverted dictionary doesn't support search element by id");
    }
}

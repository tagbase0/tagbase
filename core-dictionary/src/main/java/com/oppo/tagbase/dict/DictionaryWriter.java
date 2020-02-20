package com.oppo.tagbase.dict;


import java.io.IOException;

/**
 * Created by wujianchao on 2020/2/11.
 */
public interface DictionaryWriter {

    /**
     * add an element to dictionary
     */
    long add(byte[] element) throws IOException;

}

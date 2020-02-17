package com.oppo.tagbase.dict;


/**
 * Created by wujianchao on 2020/2/11.
 */
public interface AppendableDictionary extends Dictionary {

    /**
     * 向字典中添加一个值
     */
    int add(byte[] element);

}

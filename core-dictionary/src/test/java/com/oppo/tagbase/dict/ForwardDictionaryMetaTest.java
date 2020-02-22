package com.oppo.tagbase.dict;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by wujianchao on 2020/2/21.
 */
public class ForwardDictionaryMetaTest {

    @Test
    public void sanityTest() {

        ForwardDictionaryMeta meta = new ForwardDictionaryMeta();
        meta.setLastModifiedDate(System.currentTimeMillis());
        meta.setGroupNum(2);
        meta.setElementNum(100);

        byte[] bytes = meta.serialize();
        ForwardDictionaryMeta deserializedMeta = ForwardDictionaryMeta.deserialize(bytes);

        Assert.assertEquals(meta, deserializedMeta);
    }
}

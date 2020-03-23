package com.oppo.tagbase.dict.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;

/**
 * Created by wujianchao on 2020/2/13.
 */
public class UnsignedTypesTest {

    @Test
    public void unsignedByteTest() {
        Assert.assertEquals(1, UnsignedTypes.unsignedByte((byte) 1));
        Assert.assertEquals(Byte.MAX_VALUE, UnsignedTypes.unsignedByte(Byte.MAX_VALUE));

        BitSet bs = new BitSet(8);
        bs.set(0, 8);

        byte value = bs.toByteArray()[0];

        System.out.println(value);

        Assert.assertEquals(255, UnsignedTypes.unsignedByte((byte) -1));
        Assert.assertEquals(255, UnsignedTypes.unsignedByte(value));

    }

    @Test
    public void unsignedIntTest() {
        System.out.println(Integer.MAX_VALUE);
        System.out.println(Integer.MAX_VALUE + 1);
        Assert.assertEquals((long)Integer.MAX_VALUE + 1, UnsignedTypes.unsignedInt(Integer.MAX_VALUE + 1));
    }

    @Test
    public void castNegativeTest() {
        long a = -1L;
        int b = -1;
        long c = (long)b;
        Assert.assertEquals(a, c);
    }


}

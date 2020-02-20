package com.oppo.tagbase;

import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by wujianchao on 2020/2/20.
 */
public class ByteBufferTest {

    @Test
    public void byteBufferTest(){
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putShort((short) 1);
        System.out.println(buf.position());
        System.out.println(buf);
        buf.flip();
        System.out.println(buf);
        System.out.println(buf.array().length);
    }

}

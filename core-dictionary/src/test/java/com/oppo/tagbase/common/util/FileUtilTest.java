package com.oppo.tagbase.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by wujianchao on 2020/2/20.
 */
public class FileUtilTest {

    @Test
    public void readTest() throws IOException {

        File file = FileUtil.createDeleteOnExitFile("target/fu-read-test.txt");
        byte[] bytes = "12".getBytes();

        FileUtil.write(file, 0, bytes);

        try(FileInputStream in = new FileInputStream(file);
            FileChannel channel = in.getChannel()) {

            byte[] actuallyBytes = FileUtil.read(channel, 0, bytes.length);

            Assert.assertEquals(BytesUtil.toUTF8String(bytes), BytesUtil.toUTF8String(actuallyBytes));
        }
    }


    @Test
    public void writeTest() throws IOException {
        File file = FileUtil.createDeleteOnExitFile("target/fu-write-test.txt");
        byte[] bytes = "12".getBytes();

        FileUtil.write(file, 0, bytes);
        FileUtil.write(file, 1, bytes);

        Assert.assertEquals("112", FileUtil.toUTF8String(file));
    }

}

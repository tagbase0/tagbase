package com.oppo.tagbase.dict.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by wujianchao on 2020/2/20.
 */
public class FileUtilTest {

    @Test
    public void writeTest() throws IOException {
        File file = new File("target/a.txt");
        if(file.exists()){
            file.delete();
        }
        FileOutputStream out = new FileOutputStream(file);

        byte[] bytes = "12".getBytes();

        FileUtil.write(out, 0, bytes);
        out.flush();

        FileUtil.write(out, 1, bytes);
        FileUtil.closeQuietly(out);

        Assert.assertEquals("112", FileUtil.toUTF8String(file));
    }
}

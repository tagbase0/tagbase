package com.oppo.tagbase.common.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by wujianchao on 2020/2/20.
 */
public class FileUtil {

    public static void write(File f, long filePosition, byte[] data) throws IOException {
        if(!f.exists()) {
            f.createNewFile();
        }
        try(RandomAccessFile raf = new RandomAccessFile(f, "rw");) {
            raf.seek(filePosition);
            raf.write(data);
        }
    }

    @Deprecated
    public static void write(FileChannel channel, long filePosition, byte[] data) throws IOException {
        channel.position(filePosition);
        ByteBuffer buf = ByteBuffer.wrap(data);

        int writeLength;
        int currentLength = 0;

        while (currentLength != data.length) {
            writeLength = channel.write(buf);
            currentLength += writeLength;
        }
    }

    public static byte[] read(FileChannel in, long filePosition, int length) throws IOException {
        in.position(filePosition);

        ByteBuffer buf = ByteBuffer.allocate(length);

        int readLength;
        int currentLength = 0;

        while (currentLength != length) {
            readLength  = in.read(buf);
            if(readLength == -1) {
                throw new IOException("data less than " + length);
            }
            currentLength += readLength;
        }

        return buf.array();
    }


    public static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
        }
    }

    public static String toUTF8String(File file) throws IOException {
        long length = file.length();

        if(length >= Integer.MAX_VALUE - 8) {
            throw new IOException("file large than 2GB, actually is " + length);
        }

        try(FileInputStream in = new FileInputStream(file);
            FileChannel channel = in.getChannel()) {

            byte[] bytes = read(channel, 0, (int) file.length());
            return BytesUtil.toUTF8String(bytes);
        }
    }

    /**
     * Just for test
     */
    public static File createDeleteOnExitFile(String name) {
        File file = new File(name);
        file.deleteOnExit();
        return file;
    }



}

package com.oppo.tagbase.query.operator;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class OperatorBuffer<T> {

    LinkedBlockingQueue<T> buffer;

    int inputSourceCount;

    int currentEOFCount;

    public T next() {

        while (true) {
            T output = buffer.poll();
            if (output == Row.EOF) {
                currentEOFCount++;
                if (currentEOFCount == inputSourceCount) {
                    return null;
                }
            } else {
                return output;
            }
        }

    }


    public void offer(T bitmap) {
        buffer.offer(bitmap);
    }


}

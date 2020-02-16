package com.oppo.tagbase.storage.core.example.testobj;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class OperatorBuffer {

    LinkedBlockingQueue<TagBitmap> buffer = new LinkedBlockingQueue<>();

    int inputSourceCount;

    int currentEOFCount=0;

    public OperatorBuffer(int inputSourceCount) {
        this.inputSourceCount = inputSourceCount;
    }

    public TagBitmap next() throws InterruptedException {

        while(true) {
            TagBitmap output = buffer.take();
            if (output == TagBitmap.EOF) {
                currentEOFCount++;
                if (currentEOFCount >= inputSourceCount) {
                    return null;
                }
            }else{
                return output;
            }
        }

    }

    public boolean hasNext(){
        if (currentEOFCount < inputSourceCount) {
            return true;
        }
        return false;
    }

    public void offer(TagBitmap bitmap){
        buffer.offer(bitmap);
    }



}

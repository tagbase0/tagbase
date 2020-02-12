package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.TagBitmap;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class OperatorBuffer {

    LinkedBlockingQueue<TagBitmap> buffer;

    int inputSourceCount;

    int currentEOFCount;

    public TagBitmap next(){

        while(true) {
            TagBitmap output = buffer.poll();
            if (output == TagBitmap.EOF) {
                currentEOFCount++;
                if (currentEOFCount == inputSourceCount) {
                    return null;
                }
            }else{
                return output;
            }
        }

    }


    public void offer(TagBitmap bitmap){
        buffer.offer(bitmap);
    }



}

package com.oppo.tagbase.storage.core.obj;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by liangjingya on 2020/2/19.
 */
public class OperatorBuffer {

    LinkedBlockingQueue<AggregateRow> buffer = new LinkedBlockingQueue<>();

    int inputSourceCount;

    int currentEOFCount=0;

    public OperatorBuffer(int inputSourceCount) {
        this.inputSourceCount = inputSourceCount;
    }

    public AggregateRow next() throws InterruptedException {
        while(true) {
            AggregateRow output = buffer.take();
            if (output == AggregateRow.EOF) {
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

    public void offer(AggregateRow bitmap){
        buffer.offer(bitmap);
    }



}

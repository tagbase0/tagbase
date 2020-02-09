package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.TagBitmap;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class OperatorBuffer {

    LinkedBlockingQueue<TagBitmap> buffer;

    public boolean hasMore(){
        return false;
    }


    public TagBitmap getNext(){
        return null;
    }

    public boolean isBlocked(){
        return false;
    }

}

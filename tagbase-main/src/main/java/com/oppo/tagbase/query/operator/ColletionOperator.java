package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.TagBitmap;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class ColletionOperator {

    List<OperatorBuffer> buffers;
    String operator;

    LinkedBlockingQueue output;


    public void  work(){
        //index 0 buffer

        OperatorBuffer leftBuffer = buffers.get(0);


        TagBitmap b = leftBuffer.getNext();


        while(true){
            for(int n= 1;n<buffers.size();n++){
               if(buffers.get(n).isBlocked() && buffers.get(n).hasMore()){
                   TagBitmap c = buffers.get(n).getNext();
                   c.getBitmap().and(b.getBitmap());
                   c.setKey(joinKey(b.getKey(),c.getKey()));

                   output.offer(c);

               }


            }
        }



    }

    private String joinKey(String key, String key1) {
        return null;
    }


}

package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.Filter;
import com.oppo.tagbase.query.TagBitmap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class SingleQueryOperator {


    List<String> dim;
    String table;
    Filter filter;

    boolean needGroupby;


    LinkedBlockingQueue<TagBitmap> output;

    SingleQueryOperator(List<String> dim){
        // 如果是标签表，dim没有定义， filter条件只有一个 直接输出
        //如果是标签表，dim没有定义，filter条件有多个值 需要汇总，输出一个
        //如果是标签表， dim定义了，不论如何都可以直接输出
        //dim定义的列+filter列为=所有维度列， 且filter条件值的范围都为1





    }



    private void analysis(){



    }
    public void work(){





        Map<String,TagBitmap> map = new HashMap<>();


        // get output from storage module
        Iterator<TagBitmap> source = null;

        while(source.hasNext()){

            TagBitmap tagBitmap = source.next();

           if(map.containsKey(tagBitmap.getKey())){
               map.get(tagBitmap.getKey()).getBitmap().or(tagBitmap.getBitmap());
           }else{
               map.put(tagBitmap.getKey(),tagBitmap);
           }

        }

        // put result to output

        for(TagBitmap tagBitmap:map.values()){
            output.offer(tagBitmap);
        }
        notifyOutputEnd();




    }

    private void notifyOutputEnd() {

    }

}




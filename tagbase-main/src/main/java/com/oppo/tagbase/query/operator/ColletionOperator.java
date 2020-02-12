package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.TagBitmap;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static com.oppo.tagbase.query.TagBitmap.EOF;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class ColletionOperator implements Operator {

    List<OperatorBuffer> buffers;

    OperatorBuffer leftSource;
    OperatorBuffer rightSource;


    OperatorBuffer outputBuffer;


    String operator;

    LinkedBlockingQueue output;


    public void work() {
        //index 0 buffer


        TagBitmap b = leftSource.next();

        TagBitmap c;
        while ((c = rightSource.next()) != null) {

            c.getBitmap().and(b.getBitmap());
            c.setKey(joinKey(b.getKey(), c.getKey()));

            output.offer(c);

        }

        output.offer(EOF);


    }

    private String joinKey(String key, String key1) {
        return null;
    }


    @Override
    public OperatorBuffer getOuputBuffer() {
        return null;
    }
}

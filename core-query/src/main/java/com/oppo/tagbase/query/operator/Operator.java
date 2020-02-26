package com.oppo.tagbase.query.operator;

import java.util.function.Consumer;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
//TODO support union operator
public interface Operator extends Runnable {

    OperatorBuffer getOutputBuffer();

    void cancelOutput();

    void ifFinish(Runnable o);

    void ifException(Consumer<Exception> o);

    int getId();
    void setQueryId(String queryId);
}


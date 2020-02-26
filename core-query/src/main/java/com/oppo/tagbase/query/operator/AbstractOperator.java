package com.oppo.tagbase.query.operator;

import java.util.function.Consumer;

/**
 * @author huangfeng
 * @date 2020/2/23 17:32
 */
public abstract class AbstractOperator implements Operator {

    protected OperatorBuffer outputBuffer;
    private volatile Runnable finishCall;
    private volatile Consumer<Exception> exceptionCall;
    private Exception e;
    private int operatorId;
    private String queryId;

    public AbstractOperator(int operatorId) {
        this.operatorId = operatorId;
    }


    @Override
    public OperatorBuffer getOutputBuffer() {
        return outputBuffer;
    }

    @Override
    public void cancelOutput() {
        outputBuffer.postEnd();
    }

    @Override
    public void ifFinish(Runnable finishCallback) {
        this.finishCall = finishCallback;
    }

    @Override
    public void ifException(Consumer<Exception> exceptionCall) {
        this.exceptionCall = exceptionCall;
    }

    @Override
    public int getId() {
        return operatorId;
    }

    @Override
    public void setQueryId(String queryId){
        this.queryId = queryId;
    }

    @Override
    public void run() {
        String currThreadName = Thread.currentThread().getName();
        try {
            String queryThreadName = String.format(
                    "%s[%s-%s]",
                    currThreadName,queryId, operatorId);
            Thread.currentThread().setName(queryThreadName);

            internalRun();
        } catch (Exception e) {
            exceptionCall.accept(e);
        } finally {
            Thread.currentThread().setName(currThreadName);
        }
        //TODO finishCall can't be called after execption, and consider if it is null
        finishCall.run();
    }

    public abstract void internalRun();
}

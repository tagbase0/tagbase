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
    public void run() {
        try {
            internalRun();
        } catch (Exception e) {
            exceptionCall.accept(e);
        }

        finishCall.run();
    }

    public abstract void internalRun();
}

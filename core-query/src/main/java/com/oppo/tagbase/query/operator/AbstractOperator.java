package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.storage.core.obj.OperatorBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author huangfeng
 * @date 2020/2/23 17:32
 */
public abstract class AbstractOperator implements Operator {

    private static Logger LOG = LoggerFactory.getLogger(AbstractOperator.class);
    protected OperatorBuffer outputBuffer;

    private volatile Optional<Runnable> finishCall;
    private volatile Optional<Consumer<Exception>> exceptionCall;
    private int operatorId;
    private String queryId;

    public AbstractOperator(int operatorId,OperatorBuffer outputBuffer) {
        this.operatorId = operatorId;
        this.outputBuffer = outputBuffer;
        finishCall = Optional.empty();
        exceptionCall = Optional.empty();
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
        this.finishCall = Optional.of(finishCallback);
    }

    @Override
    public void ifException(Consumer<Exception> exceptionCall) {
        this.exceptionCall = Optional.of(exceptionCall);
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
        boolean happenException = false;
        try {
            String queryThreadName = String.format(
                    "%s[%s-%s]",
                    currThreadName,queryId, operatorId);
            Thread.currentThread().setName(queryThreadName);

            internalRun();
        } catch (Exception e) {
            happenException = true;
            exceptionCall.ifPresent(callback -> callback.accept(e));
            LOG.error("operator execute fail:",e);
        } finally {
            Thread.currentThread().setName(currThreadName);
        }
        if(!happenException) {
            finishCall.ifPresent(callback -> callback.run());
        }
    }

    public abstract void internalRun();
}

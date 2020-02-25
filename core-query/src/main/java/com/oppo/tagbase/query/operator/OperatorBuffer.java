package com.oppo.tagbase.query.operator;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author huangfeng
 * @date 2020/2/8
 * <p>
 * just abstract message middle buffer , therefore can't limit on producer and consumer
 * for example, one produce can send many EOF. in the case，producer and consumer must follow the rule of Operator.
 * 1. consumer only one , produce can be more than zero
 * 2. end just can be called once for a consumer
 */
public class OperatorBuffer<T> {

    //TODO limit data size（bitmap）
    private LinkedBlockingQueue<Message<T>> buffer = new LinkedBlockingQueue<>();

    private int inputSourceCount;
    private AtomicInteger currentEOFCount = new AtomicInteger(0);
    private int consumeEOFCount;

    private volatile boolean isClose;
    private volatile Exception exception;

    public OperatorBuffer(int inputSourceCount) {
        this.inputSourceCount = inputSourceCount;
        buffer = new LinkedBlockingQueue<>();
    }

    public OperatorBuffer() {
        this.inputSourceCount = 1;
    }


    public T next() {
        try {
            while (true) {

                Message<T> output = buffer.take();
                if (exception != null) {
                    throw exception;
                }
                if (isClose) {
                    return null;
                }

                if (output == EOF) {
                    consumeEOFCount++;
                    if (consumeEOFCount == inputSourceCount) {
                        isClose = true;
                        return null;
                    }

                } else {
                    return output.data;
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    public void postData(T data) {
        if (exception != null || isClose) {
            throw new RuntimeException("Operator has been closed");
        }

        buffer.offer(new Message<>(data));
    }

    public void postEnd() {
        if (exception != null || isClose) {
            throw new RuntimeException("Operator has been closed");
        }
        buffer.offer(EOF);
        currentEOFCount.incrementAndGet();
    }


    public void fastFail(Exception e) {
        this.exception = e;
        buffer.clear();
        buffer.offer(EOF);
    }

    public void cancel() {
        isClose = true;
        buffer.clear();
        buffer.offer(EOF);
    }

    private void clear() {
        buffer.clear();
    }


    public boolean isInputFinished() {
        return currentEOFCount.intValue() >= inputSourceCount;
    }


    private static final Message EOF = new Message();

    private static class Message<T> {
        private T data;

        Message() {
        }

        Message(T data) {
            this.data = data;
        }

    }

}

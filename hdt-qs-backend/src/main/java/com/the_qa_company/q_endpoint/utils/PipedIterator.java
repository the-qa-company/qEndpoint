package com.the_qa_company.q_endpoint.utils;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public class PipedIterator<T> implements Iterator<T> {
    public static class PipedIteratorException extends RuntimeException {
        public PipedIteratorException(String message, Throwable t) {
            super(message, t);
        }
    }

    @FunctionalInterface
    public interface PipeCallBack<T> {
        void createPipe(PipedIterator<T> pipe) throws Exception;
    }

    /**
     * create a piped iterator from a callback runner, the call to the callback should be made in the callbackRunner
     * @param callbackRunner the callback runner
     * @param <T> type of the iterator
     * @return the iterator
     */
    public static <T> PipedIterator<T> createOfCallback(PipeCallBack<T> callbackRunner) {
        PipedIterator<T> pipe = new PipedIterator<>(10000);

        Thread thread = new Thread(() -> {
            try {
                callbackRunner.createPipe(pipe);
                pipe.closePipe();
            } catch (Exception e) {
                pipe.closePipe(e);
            }
        }, "PipeIterator");
        thread.start();

        return pipe;
    }

    private class PipedNode {
        T t;

        public PipedNode(T t) {
            this.t = t;
        }

        boolean end() {
            return false;
        }
    }

    private class PipedNodeEnd extends PipedNode {
        private final Exception exception;
        public PipedNodeEnd(Exception exception) {
            super(null);
            this.exception = exception;
        }

        boolean end() {
            if (exception != null)
                throw new PipedIteratorException("Crash while creating pipe", exception);
            return true;
        }
    }

    private final ArrayBlockingQueue<PipedNode> queue;
    private PipedNode next;

    public PipedIterator(int bufferSize) {
        queue = new ArrayBlockingQueue<>(bufferSize);
    }

    /**
     * add an element to the piped iterator
     * @param element the element to pipe
     * @throws PipedIteratorException in case of Interruption
     */
    public void addElement(T element) {
        try {
            queue.put(new PipedNode(element));
        } catch (InterruptedException e) {
            throw new PipedIteratorException("Can't add element", e);
        }
    }

    /**
     * close the pipe after the last added element
     * @throws PipedIteratorException in case of Interruption
     */
    public void closePipe() {
        closePipe(null);
    }
    /**
     * close the pipe after the last added element
     * @param e exception to call at the {@link #hasNext()} next call
     * @throws PipedIteratorException in case of Interruption
     */
    public void closePipe(Exception e) {
        try {
            queue.put(new PipedNodeEnd(e));
        } catch (InterruptedException ie) {
            throw new PipedIteratorException("Can't close pipe", ie);
        }
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            try {
                next = queue.take();
            } catch (InterruptedException e) {
                throw new PipedIteratorException("Can't get next element", e);
            }
        }
        return !next.end();
    }

    @Override
    public T next() {
        if (!hasNext())
            return null;
        T next = this.next.t;
        this.next = null;
        return next;
    }
}

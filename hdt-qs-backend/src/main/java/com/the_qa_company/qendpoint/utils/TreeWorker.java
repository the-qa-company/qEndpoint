package com.the_qa_company.qendpoint.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * a worker to parse tree operation
 *
 * @param <T>
 *            the type used in the tree
 *
 * @author Antoine Willerval
 */
public class TreeWorker<T> {
    private static final AtomicInteger JOB_ID_NAME = new AtomicInteger();
    private final TreeWorkerCat<T> catFunction;
    private final Supplier<T> baseLevelSupplier;
    private final Consumer<T> delete;
    private int maxLevel = 0;
    private final Object WAITING_SYNC = new Object() {
    };
    private int workerWaiting = 0;
    private final List<Element> elements = new ArrayList<>();
    private final List<Worker> workers;
    private boolean started = false;
    private boolean done = false;
    private TreeWorkerException throwable;

    /**
     * create a tree worker
     *
     * @param catFunction
     *            the function to cat 2 nodes
     * @param baseLevelSupplier
     *            the supplier to get base nodes
     * @param delete
     *            the delete method to delete data in case of error, can be null if no delete is required
     *
     * @throws TreeWorkerException
     *             if the tree worker can't be created
     * @throws java.lang.NullPointerException
     *             if catFunction or baseLevelSupplier is null
     */
    public TreeWorker(TreeWorkerCat<T> catFunction, Supplier<T> baseLevelSupplier, Consumer<T> delete)
            throws TreeWorkerException {
        this(catFunction, baseLevelSupplier, delete, Runtime.getRuntime().availableProcessors());
    }

    /**
     * create a tree worker
     *
     * @param catFunction
     *            the function to cat 2 nodes
     * @param baseLevelSupplier
     *            the supplier to get base nodes
     * @param delete
     *            the delete method to delete data in case of error, can be null if no delete is required
     * @param workers
     *            the number of workers to use
     *
     * @throws TreeWorkerException
     *             if the tree worker can't be created
     * @throws java.lang.NullPointerException
     *             if catFunction or baseLevelSupplier is null
     */
    public TreeWorker(TreeWorkerCat<T> catFunction, Supplier<T> baseLevelSupplier, Consumer<T> delete, int workers)
            throws TreeWorkerException {
        this.catFunction = Objects.requireNonNull(catFunction, "catFunction can't be null!");
        this.baseLevelSupplier = Objects.requireNonNull(baseLevelSupplier, "baseLevelSupplier can't be null!");
        if (delete == null) {
            this.delete = (t) -> {
            };
        } else {
            this.delete = delete;
        }
        if (workers <= 0) {
            throw new TreeWorkerException("worker count can't be <= 0!");
        }
        T t = baseLevelSupplier.get();
        if (t == null) {
            throw new TreeWorkerException("no base element!");
        }
        elements.add(new Element(t, 0));
        this.workers = new ArrayList<>(workers);
        for (int i = 0; i < workers; i++) {
            this.workers.add(new Worker());
        }
    }

    public void start() {
        synchronized (elements) {
            if (started) {
                throw new IllegalArgumentException("TreeWorker already started!");
            }
            for (Worker worker : this.workers) {
                worker.start();
            }
            started = true;
        }
    }

    private void clearData() {
        for (Element e : elements) {
            delete.accept(e.t);
        }
    }

    /**
     * wait for the tree worker to complete
     *
     * @return the last element
     *
     * @throws TreeWorkerException
     *             if an error occurred in a worker
     * @throws InterruptedException
     *             in case of interruption
     */
    public T waitToComplete() throws TreeWorkerException, InterruptedException {
        try {
            for (Worker w : workers) {
                w.join();
            }
        } catch (InterruptedException e) {
            clearData();
            throw e;
        }

        if (throwable != null) {
            clearData();
            throw throwable;
        }

        if (!done) {
            clearData();
            // shouldn't be possible?
            throw new TreeWorkerException("The worker isn't done!");
        }
        if (elements.isEmpty()) {
            return null;
        }
        return elements.get(0).t;
    }

    private int countLevel(int level) {
        int c = 0;
        for (Element e : elements) {
            if (e.level == level) {
                c++;
            }
        }
        return c;
    }

    private Tuple searchDir(int start, int direction, int min) {
        if (direction < 0) {
            for (int i = start; i >= 0; i--) {
                Tuple tuple = searchAtLevel(i);
                if (tuple.size() >= min) {
                    return tuple;
                }
            }
        } else {
            for (int i = start; i <= maxLevel; i++) {
                Tuple tuple = searchAtLevel(i);
                if (tuple.size() >= min) {
                    return tuple;
                }
            }
        }
        return null;
    }

    private Tuple searchAtLevel(int level) {
        synchronized (elements) {
            Element old = null;
            for (Element e : elements) {
                if (e.level == level) {
                    if (old != null) {
                        return new Tuple(old, e);
                    } else {
                        old = e;
                    }
                }
            }
            return new Tuple(old, null);
        }
    }

    /**
     * cat function to merge two elements
     *
     * @param <T>
     *            the elements type
     *
     * @author Antoine Willerval
     */
    @FunctionalInterface
    public interface TreeWorkerCat<T> {
        T construct(T a, T b);
    }

    /**
     * @return if the worker is completed
     */
    public boolean isCompleted() {
        synchronized (elements) {
            return (done && elements.size() <= 1) || throwable != null;
        }
    }

    private class Element {
        T t;
        int level;

        public Element(T t, int level) {
            this.t = t;
            this.level = level;
        }
    }

    private class Tuple {
        Element a, b;

        public Tuple(Element a, Element b) {
            this.a = a;
            this.b = b;
        }

        public void remove() throws TreeWorkerException {
            if (a != null) {
                if (!elements.remove(a)) {
                    throw new TreeWorkerException("Can't remove a from elements!");
                }
            }
            if (b != null) {
                if (!elements.remove(b)) {
                    throw new TreeWorkerException("Can't remove b from elements!");
                }
            }
        }

        public int size() {
            if (a == null) {
                return 0;
            }
            if (b == null) {
                return 1;
            }
            return 2;
        }
    }

    private abstract static class TreeWorkerJob {
        abstract void runJob();

        void clear() {
        }
    }

    private final Object FETCH_SYNC = new Object() {
    };

    private class Fetch extends TreeWorkerJob {
        @Override
        public void runJob() {
            synchronized (FETCH_SYNC) {
                if (done) {
                    return; // another fetch job won
                }
                T t = baseLevelSupplier.get();
                synchronized (elements) {
                    if (t == null) {
                        done = true;
                    } else {
                        elements.add(new Element(t, 0));
                    }
                    elements.notifyAll();
                }
            }
        }
    }

    private class Merge extends TreeWorkerJob {
        Element a, b;

        public Merge(Element a, Element b) {
            this.a = a;
            this.b = b;
            assert a.level == b.level : "cat elements from different level!";
        }

        @Override
        public void runJob() {
            T t = catFunction.construct(a.t, b.t);
            synchronized (elements) {
                elements.add(new Element(t, a.level + 1));
                maxLevel = Math.max(maxLevel, a.level + 1);
            }
        }

        @Override
        void clear() {
            delete.accept(a.t);
            delete.accept(b.t);
        }
    }

    private class Worker extends Thread {
        public Worker() {
            super("JobWorker#" + JOB_ID_NAME.incrementAndGet());
        }

        @Override
        public void run() {
            while (!isCompleted()) {
                TreeWorkerJob job = null;
                try {
                    synchronized (WAITING_SYNC) {
                        job = getJob();
                        if (job == null) {
                            if (isCompleted()) {
                                return;
                            }
                            workerWaiting++;
                            WAITING_SYNC.wait();
                            --workerWaiting;
                            continue;
                        }
                    }
                    job.runJob();
                    synchronized (WAITING_SYNC) {
                        if (workerWaiting > 0) {
                            WAITING_SYNC.notify();
                        }
                    }
                } catch (Throwable t) {
                    if (job != null) {
                        job.clear();
                    }
                    synchronized (elements) {
                        if (t instanceof TreeWorkerException) {
                            throwable = (TreeWorkerException) t;
                        } else {
                            throwable = new TreeWorkerException(t);
                        }
                        elements.notifyAll();
                    }
                    synchronized (WAITING_SYNC) {
                        WAITING_SYNC.notifyAll();
                    }
                }
            }
        }

        private TreeWorkerJob getJob() throws TreeWorkerException {
            synchronized (elements) {
                while (true) {
                    if (done) {
                        if (elements.size() == 1) {
                            return null; // end, no ascend/merge required
                        }
                        Tuple w = searchDir(0, 1, 1);
                        if (w == null) {
                            return null; // size == 0 end
                        } else if (w.size() == 1) {
                            w.a.level++;
                        } else { // size == 2
                            w.remove();
                            return new Merge(w.a, w.b);
                        }
                    } else {
                        int level0 = countLevel(0);
                        if (workers.size() != 1 && level0 < workers.size() / 2) {
                            return new Fetch();
                        }
                        Tuple w = searchDir(maxLevel, -1, 2);
                        if (w == null) {
                            return new Fetch();
                        } else {
                            w.remove();
                            return new Merge(w.a, w.b);
                        }
                    }
                }
            }
        }
    }

    /**
     * An exception in the tree worker
     *
     * @author Antoine Willerval
     */
    public static class TreeWorkerException extends Exception {
        public TreeWorkerException(Throwable cause) {
            super(cause);
        }

        public TreeWorkerException(String message) {
            super(message);
        }
    }
}

package com.the_qa_company.q_endpoint.utils;

import org.rdfhdt.hdt.triples.TripleString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.function.Consumer;

public class FileTripleIterator implements Iterator<TripleString> {
    private final Iterator<TripleString> it;
    private final long maxSize;
    private long currentSize = 0L;
    private TripleString next;
    private boolean stop = false;

    public FileTripleIterator(Iterator<TripleString> it, long maxSize) {
        this.it = it;
        this.maxSize = maxSize;
    }

    private long estimateTripleSize(TripleString triple) {
        try {
            return triple.asNtriple().toString().getBytes(StandardCharsets.UTF_8).length;
        } catch (IOException e) {
            throw new RuntimeException("Can't estimate the size of the triple " + triple, e);
        }
    }

    @Override
    public boolean hasNext() {
        if (stop)
            return false;

        if (next != null)
            return true;

        if (it.hasNext()) {
            next = it.next();
            long estimation = estimateTripleSize(next);
            if (currentSize + estimation >= maxSize) {
                stop = true;
                currentSize = estimation;
                return false;
            }

            currentSize += estimation;
            return true;
        }
        return false;
    }

    @Override
    public TripleString next() {
        TripleString t = next;
        next = null;
        return t;
    }

    @Override
    public void remove() {
        it.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super TripleString> action) {
        it.forEachRemaining(action);
    }

    /**
     * @return if we need to open a new file
     */
    public boolean hasNewFile() {
        stop = false;
        return hasNext();
    }
}

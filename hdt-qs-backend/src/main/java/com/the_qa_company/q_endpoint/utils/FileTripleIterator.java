package com.the_qa_company.q_endpoint.utils;

import org.rdfhdt.hdt.triples.TripleString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Iterator to split an iterator stream into multiple files, the iterator return {@link #hasNext()} == true once the
 * first file is returned, then the {@link #hasNewFile()} should be called to check if another file can be created and
 * re-allow {@link #hasNext()} to return true
 * @author Antoine Willerval
 */
public class FileTripleIterator implements Iterator<TripleString> {
    private final Iterator<TripleString> it;
    private final long maxSize;
    private long currentSize = 0L;
    private TripleString next;
    private boolean stop = false;

    /**
     * create a file triple iterator from a TripleString stream and a max size
     * @param it the triple iterator
     * @param maxSize the maximum size of each file, this size is estimated, so files can be bigger.
     */
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

package com.the_qa_company.qendpoint.utils;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Utility class to map an iterator to another type of iterator
 * @param <T> the input Iterator type
 * @param <R> the output Iterator type
 * @author Antoine Willerval
 */
public class MapIterator<T, R> implements Iterator<R> {
    private final Function<T, R> function;
    private final Iterator<T> it;

    /**
     * create an map iterator
     * @param iterator the iterator to map
     * @param function the function to map the iterator
     * @throws java.lang.NullPointerException if the iterator or the function is null
     */
    public MapIterator(Iterator<T> iterator, Function<T, R> function) {
        this.it = Objects.requireNonNull(iterator, "iterator can't be null!");
        this.function = Objects.requireNonNull(function, "function can't be null!");
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public R next() {
        return function.apply(it.next());
    }

    @Override
    public void remove() {
        it.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super R> action) {
        it.forEachRemaining(e -> action.accept(function.apply(e)));
    }
}

package com.the_qa_company.q_endpoint.utils;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public class MapIterator<T, R> implements Iterator<R> {
    private final Function<T, R> function;
    private final Iterator<T> it;

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

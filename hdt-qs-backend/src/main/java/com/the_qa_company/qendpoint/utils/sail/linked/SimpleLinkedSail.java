package com.the_qa_company.qendpoint.utils.sail.linked;

import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.helpers.SailWrapper;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Class to store multiple sails
 *
 * @param <S>
 *            the linked sail type
 *
 * @author Antoine Willerval
 */
public class SimpleLinkedSail<S extends Sail> implements LinkedSail<S> {
    /**
     * create linked sails from sails
     *
     * @param sails
     *            the sails to link
     * @param chainMethod
     *            the link method
     * @param <S>
     *            the sails type
     *
     * @return the linked sail
     */
    public static <S extends Sail> LinkedSail<S> linkSails(List<S> sails, BiConsumer<S, Sail> chainMethod) {
        return linkSails(sails.stream(), chainMethod);
    }

    /**
     * create linked sails from sails
     *
     * @param sails
     *            the sails to link
     * @param chainMethod
     *            the link method
     * @param <S>
     *            the sails type
     *
     * @return the linked sail
     */
    public static <S extends Sail> LinkedSail<S> linkSails(Stream<S> sails, BiConsumer<S, Sail> chainMethod) {
        Iterator<S> it = sails.iterator();

        if (!it.hasNext()) {
            throw new IllegalArgumentException("empty sails");
        }

        S first = it.next();
        S last = first;

        // chain the sails
        while (it.hasNext()) {
            S next = it.next();
            chainMethod.accept(last, next);
            last = next;
        }
        final S lastNode = last;

        // chain the last sail with the future next sail
        return new SimpleLinkedSail<>(first, (sail) -> chainMethod.accept(lastNode, sail));
    }

    /**
     * create linked sails from sails
     *
     * @param sails
     *            the sails to link
     *
     * @return the linked sail
     */
    public static <LS extends LinkedSail<? extends NotifyingSail>> LinkedSail<? extends NotifyingSail> linkSails(
            Stream<LS> sails) {
        Iterator<LS> it = sails.iterator();

        if (!it.hasNext()) {
            throw new IllegalArgumentException("empty sails");
        }

        LS first = it.next();
        LS last = first;

        // chain the sails
        while (it.hasNext()) {
            LS next = it.next();
            last.getSailConsumer().accept(next.getSail());
            last = next;
        }
        final LS lastNode = last;

        // chain the last sail with the future next sail
        return new SimpleLinkedSail<>(first.getSail(), (sail) -> lastNode.getSailConsumer().accept(sail));
    }

    /**
     * create a linked sail from a sail wrapper
     *
     * @param wrapper
     *            the wrapper
     * @param <S>
     *            the wrapper type
     *
     * @return the linked sail
     */
    public static <S extends SailWrapper> LinkedSail<S> ofWrapper(S wrapper) {
        return new SimpleLinkedSail<>(wrapper, wrapper::setBaseSail);
    }

    private final S sail;
    private final Consumer<Sail> sailConsumer;

    /**
     * create from a sail and an end sail consumer
     *
     * @param sail
     *            the first sail
     * @param sailConsumer
     *            a consumer to link the last sail
     */
    public SimpleLinkedSail(S sail, Consumer<Sail> sailConsumer) {
        this.sail = sail;
        this.sailConsumer = sailConsumer;
    }

    /**
     * copy constructor
     *
     * @param other
     *            the other sails to copy
     */
    protected SimpleLinkedSail(LinkedSail<? extends S> other) {
        this.sail = other.getSail();
        this.sailConsumer = other.getSailConsumer();
    }

    /**
     * @return the described sail
     */
    @Override
    public S getSail() {
        return sail;
    }

    /**
     * @return the consumer to set the base sail
     */
    @Override
    public Consumer<Sail> getSailConsumer() {
        return sailConsumer;
    }
}

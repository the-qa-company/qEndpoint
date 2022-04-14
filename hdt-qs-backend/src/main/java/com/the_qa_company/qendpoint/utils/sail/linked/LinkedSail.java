package com.the_qa_company.qendpoint.utils.sail.linked;

import org.eclipse.rdf4j.sail.Sail;

import java.util.function.Consumer;

/**
 * Class to store multiple sails
 *
 * @param <S> the linked sail type
 * @author Antoine Willerval
 */
public interface LinkedSail<S extends Sail> {
	/**
	 * @return the described sail
	 */
	S getSail();


	/**
	 * @return the consumer to set the base sail
	 */
	Consumer<Sail> getSailConsumer();
}

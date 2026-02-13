package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.function.Supplier;

/**
 * A pull-based supplier that also reports how much input it consumed. Used by
 * disk loaders to budget chunk sizes and track raw input sizes.
 */
public interface SizedSupplier<E> extends Supplier<E> {
	/**
	 * @return estimated input size consumed by this supplier (typically bytes).
	 */
	long getSize();
}

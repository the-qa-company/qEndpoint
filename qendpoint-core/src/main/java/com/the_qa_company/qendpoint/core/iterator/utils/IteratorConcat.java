/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package com.the_qa_company.qendpoint.core.iterator.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Iterator of Iterators */

public class IteratorConcat<T> implements Iterator<T> {
	// No - we don't really need IteratorCons and IteratorConcat
	// Historical.

	private final List<Iterator<T>> iterators = new ArrayList<>();
	int idx = -1;
	private Iterator<T> current;
	private Iterator<T> removeFrom;
	boolean finished;

	public static <T> Iterator<T> concat(Iterator<T> iter1, Iterator<T> iter2) {
		if (iter2 == null)
			return iter1;
		if (iter1 == null)
			return iter2;
		IteratorConcat<T> c = new IteratorConcat<>();
		c.add(iter1);
		c.add(iter2);
		return c;
	}

	public static <T> Iterable<T> concat(final Iterable<T> iter1, final Iterable<T> iter2) {
		return new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return IteratorConcat.concat(iter1.iterator(), iter2.iterator());
			}
		};
	}

	public void add(Iterator<T> iter) {
		iterators.add(iter);
	}

	@Override
	public boolean hasNext() {
		if (finished)
			return false;

		if (current != null && current.hasNext())
			return true;

		while (idx < iterators.size() - 1) {
			idx++;
			current = iterators.get(idx);
			if (current.hasNext())
				return true;
			// Nothing here - move on.
			current = null;
		}
		// idx has run off the end.
		return false;
	}

	@Override
	public T next() {
		if (!hasNext())
			throw new NoSuchElementException();
		removeFrom = current;
		return current.next();
	}

	@Override
	public void remove() {
		if (null == removeFrom)
			throw new IllegalStateException("no calls to next() since last call to remove()");

		removeFrom.remove();
		removeFrom = null;

	}

}

package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * Implementation of a List using Array List, can be sorted in parallel without
 * needing to reallocate the array, can only add element to the end, no remove
 *
 * @param <T> list type
 * @author Antoine Willerval
 */
public class ParallelSortableArrayList<T> implements List<T> {
	public static final double GROW_FACTOR = 1.5f;
	private int used;
	private T[] array;
	private final Class<T[]> type;

	public ParallelSortableArrayList(Class<T[]> type) {
		this(type, 16);
	}

	@SuppressWarnings("unchecked")
	public ParallelSortableArrayList(Class<T[]> type, int capacity) {
		this.type = type;
		array = (T[]) Array.newInstance(type.getComponentType(), capacity);
	}

	private void checkSize(int newSize) {
		if (newSize >= array.length) {
			// don't allocate beyond the max size
			int allocate = (int) Math.min(Integer.MAX_VALUE - 5L, (long) (newSize * GROW_FACTOR));
			array = Arrays.copyOf(array, allocate, type);
		}
	}

	@Override
	public boolean add(T element) {
		checkSize(used + 1);
		array[used++] = element;
		return true;
	}

	@Override
	public boolean remove(Object o) {
		throw new NotImplementedException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new NotImplementedException();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		throw new NotImplementedException();
	}

	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		throw new NotImplementedException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new NotImplementedException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new NotImplementedException();
	}

	@Override
	public int size() {
		return used;
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean contains(Object o) {
		throw new NotImplementedException();
	}

	@Override
	public void clear() {
		for (int i = 0; i < used; i++) {
			array[i] = null;
		}
		used = 0;
	}

	@Override
	public T get(int index) {
		return array[index];
	}

	@Override
	public T set(int index, T element) {
		return array[index] = element;
	}

	@Override
	public void add(int index, T element) {
		throw new NotImplementedException();
	}

	@Override
	public T remove(int index) {
		throw new NotImplementedException();
	}

	@Override
	public int indexOf(Object o) {
		for (int i = 0; i < size(); i++) {
			if (get(i).equals(o)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public int lastIndexOf(Object o) {
		for (int i = size() - 1; i >= 0; i--) {
			if (get(i).equals(o)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public ListIterator<T> listIterator() {
		throw new NotImplementedException();
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		throw new NotImplementedException();
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		throw new NotImplementedException();
	}

	public T[] getArray() {
		return array;
	}

	@Override
	public Iterator<T> iterator() {
		return Arrays.asList(array).subList(0, used).iterator();
	}

	@Override
	public Object[] toArray() {
		return Arrays.copyOf(array, used, Object[].class);
	}

	@Override
	public <T1> T1[] toArray(T1[] a) {
		throw new NotImplementedException();
	}

	@Override
	public void sort(Comparator<? super T> comparator) {
		Arrays.sort(array, 0, used, comparator);
	}

	/**
	 * sort this array in parallel (if available)
	 *
	 * @param comparator sort comparator
	 */
	public void parallelSort(Comparator<T> comparator) {
		Arrays.parallelSort(array, 0, used, comparator);
	}
}

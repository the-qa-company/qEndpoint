package com.the_qa_company.qendpoint.core.util.concurrent;

import java.util.ArrayList;
import java.util.List;

/**
 * data structure to store chunk with a height, this class isn't thread safe
 *
 * @param <E> tree type
 * @author Antoine Willerval
 */
public class HeightTree<E> {
	private final List<List<E>> elements = new ArrayList<>();
	private int size = 0;

	/**
	 * get at least minNumber from the same height
	 *
	 * @param minNumber minNumber the minimum element count to get
	 * @return list containing the elements or null if at least minNumber
	 *         elements can't be get
	 */
	public List<E> getMax(int minNumber) {
		for (int i = 0; i < elements.size(); i++) {
			List<E> list = elements.get(i);
			if (list.size() == minNumber) {
				// get all the elements
				elements.set(i, new ArrayList<>());
				size -= list.size();
				return list;
			} else if (list.size() > minNumber) {
				// remove the last maxNumber elements (no copy)
				List<E> list1 = new ArrayList<>();
				int count = list.size() - 1;
				for (int j = 0; j < minNumber; j++) {
					list1.add(list.get(count - j));
					list.remove(count - j);
				}
				size -= list1.size();
				return list1;
			}
		}
		return null;
	}

	/**
	 * get at most maxNumber from the bottom height
	 *
	 * @param maxNumber the maximum element to get
	 * @return list containing the elements
	 */
	public List<E> getAll(int maxNumber) {
		List<E> list = new ArrayList<>();

		int count = maxNumber;

		for (List<E> l : elements) {
			int toGet = Math.min(l.size(), count);
			int n = l.size() - 1;
			for (int j = 0; j < toGet; j++) {
				list.add(l.get(n - j));
				l.remove(n - j);
				--count;
			}
			if (count == 0) {
				break;
			}
		}

		size -= list.size();
		return list;
	}

	/**
	 * add an element to the tree
	 *
	 * @param element element
	 * @param height  height
	 */
	public void addElement(E element, int height) {
		if (height >= elements.size()) {
			// ensure size
			for (int i = elements.size(); i <= height; i++) {
				elements.add(new ArrayList<>());
			}
		}

		size++;
		elements.get(height).add(element);
	}

	/**
	 * @return the number of elements in the tree
	 */
	public int size() {
		return size;
	}
}

package com.the_qa_company.qendpoint.core.dictionary.impl.utilCat;

import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * @author Ali Haidar
 */
public class CatUnion implements Iterator<CatElement> {
	List<IteratorPlusElement> list;

	private final List<Iterator<CatElement>> listIters;

	public CatUnion(List<Iterator<CatElement>> listIters) {

		this.list = new ArrayList<>();
		this.listIters = new ArrayList<>(listIters);
		int count = 0;
		for (Iterator<CatElement> iter : this.listIters) {
			if (iter.hasNext()) {
				CatElement element = iter.next();
				list.add(new IteratorPlusElement(count, element));
			}
			count++;
		}

	}

	@Override
	public boolean hasNext() {
		return list.size() > 0;
	}

	@Override
	public CatElement next() {
		List<CatElement.IteratorPlusPosition> ids = new ArrayList<>();
		list.sort(IteratorPlusElement::compareTo);
		ByteString element = list.get(0).element.entity;
		ListIterator<IteratorPlusElement> iteratorPlusElementIterator = list.listIterator();
		while (iteratorPlusElementIterator.hasNext()) {
			IteratorPlusElement next = iteratorPlusElementIterator.next();
			if (element.equals(next.element.entity)) {
				int iter = next.iter;
				ids.addAll(next.element.IDs);
				if (listIters.get(iter).hasNext()) {
					CatElement inElt = listIters.get(iter).next();
					iteratorPlusElementIterator.set(new IteratorPlusElement(iter, inElt));
				} else {
					iteratorPlusElementIterator.remove();
				}
			} else {
				break;
			}
		}
		return new CatElement(element, ids);
	}
}

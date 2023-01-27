package com.the_qa_company.qendpoint.core.dictionary.impl.utilCat;

public class IteratorPlusElement implements Comparable<IteratorPlusElement> {
	int iter;
	CatElement element;

	IteratorPlusElement(int iter, CatElement element) {
		this.iter = iter;
		this.element = element;
	}

	@Override
	public int compareTo(IteratorPlusElement o) {
		return element.entity.compareTo(o.element.entity);
	}
}

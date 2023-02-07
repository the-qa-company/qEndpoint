package com.the_qa_company.qendpoint.core.dictionary.impl.utilCat;

import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CatWrapper implements Iterator<CatElement> {
	public Iterator<? extends CharSequence> sectionIter;
	public ByteString iterName;
	int count = 0;

	public CatWrapper(Iterator<? extends CharSequence> sectionIter, ByteString iterName) {
		this.sectionIter = sectionIter;
		this.iterName = iterName;
	}

	@Override
	public boolean hasNext() {
		return sectionIter.hasNext();
	}

	@Override
	public CatElement next() {
		ByteString entity = ByteString.of(sectionIter.next());
		count++;
		List<CatElement.IteratorPlusPosition> IDs = new ArrayList<>();

		IDs.add(new CatElement.IteratorPlusPosition(iterName, count));
		return new CatElement(entity, IDs);
	}
}

/**
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Ali Haidar:
 * ali.haidar@qanswer.eu
 */

package com.the_qa_company.qendpoint.core.dictionary.impl.utilCat;

import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;

import java.util.ArrayList;
import java.util.Iterator;

public class CatIntersection implements Iterator<CatElement> {
	ArrayList<IteratorPlusElement> list;
	private final Iterator<CatElement> it1;
	private final Iterator<CatElement> it2;

	boolean hasNext = false;
	CatElement next;

	public CatIntersection(Iterator<CatElement> it1, Iterator<CatElement> it2) {
		this.it1 = it1;
		this.it2 = it2;
		list = new ArrayList<>();
		if (it1.hasNext()) {
			list.add(new IteratorPlusElement(1, it1.next()));
		}
		if (it2.hasNext()) {
			list.add(new IteratorPlusElement(2, it2.next()));
		}
		helpNext();
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public CatElement next() {
		CatElement r = next;
		hasNext = false;
		helpNext();
		return r;
	}

	private void helpNext() {
		while (list.size() != 0) {
			list.sort(IteratorPlusElement::compareTo);
			if (list.size() == 2) {

				if (list.get(0).element.entity.equals(list.get(1).element.entity)) {
					hasNext = true;
					ArrayList<CatElement.IteratorPlusPosition> ids = new ArrayList<>();
					ids.addAll(list.get(0).element.IDs);
					ids.addAll(list.get(1).element.IDs);

					next = new CatElement(list.get(0).element.entity, ids);
					if (it1.hasNext()) {
						list.set(0, new IteratorPlusElement(1, it1.next()));
					} else {
						list.remove(0);
						break;
					}
					if (it2.hasNext()) {
						list.set(1, new IteratorPlusElement(2, it2.next()));
					} else {
						list.remove(0);
						break;
					}
					break;
				} else {
					if (list.get(0).iter == 1) {
						if (it1.hasNext()) {
							list.set(0, new IteratorPlusElement(list.get(0).iter, it1.next()));
						} else {
							list.remove(0);
							break;
						}
					} else {
						if (it2.hasNext()) {
							list.set(0, new IteratorPlusElement(list.get(0).iter, it2.next()));
						} else {
							list.remove(0);
							break;
						}
					}
				}
			} else if (list.size() == 1) {
				list.remove(0);
			}
		}
	}

	@Override
	public void remove() {
		throw new NotImplementedException();
	}
}

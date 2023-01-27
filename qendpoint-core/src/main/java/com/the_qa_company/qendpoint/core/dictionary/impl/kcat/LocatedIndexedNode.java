package com.the_qa_company.qendpoint.core.dictionary.impl.kcat;

import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.string.ByteString;

public class LocatedIndexedNode extends IndexedNode {
	private final int hdt;

	public LocatedIndexedNode(int hdt, long index, ByteString string) {
		super(string, index);
		this.hdt = hdt;
	}

	public int getHdt() {
		return hdt;
	}

	public int compareTo(LocatedIndexedNode o) {
		return super.compareTo(o);
	}

	@Override
	public LocatedIndexedNode clone() {
		return (LocatedIndexedNode) super.clone();
	}

	@Override
	public String toString() {
		return "LocatedIndexedNode{" + "hdt=" + hdt + ", index=" + getIndex() + ", node=" + getNode() + '}';
	}
}

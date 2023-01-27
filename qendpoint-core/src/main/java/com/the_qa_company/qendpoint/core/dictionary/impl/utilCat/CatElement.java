package com.the_qa_company.qendpoint.core.dictionary.impl.utilCat;

import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.util.ArrayList;
import java.util.List;

public class CatElement {
	public ByteString entity;
	public List<IteratorPlusPosition> IDs;

	public CatElement(ByteString entity, List<IteratorPlusPosition> IDs) {
		this.entity = entity;
		this.IDs = new ArrayList<>(IDs);
	}

	public static class IteratorPlusPosition {
		public ByteString iter;
		public long pos;

		public IteratorPlusPosition(ByteString iter, long pos) {
			this.iter = iter;
			this.pos = pos;
		}
	}

}

/**
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Dennis
 * Diefenbach: dennis.diefenbach@univ-st-etienne.fr Jose Gimenez Garcia:
 * jose.gimenez.garcia@univ-st-etienne.fr
 */

package com.the_qa_company.qendpoint.core.dictionary.impl.utilCat;

import com.the_qa_company.qendpoint.core.util.disk.LongArrayDisk;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author Dennis Diefenbach &amp; Jose Gimenez Garcia
 */
public class CatMappingBack implements Closeable {
	private final long size;
	private final LongArrayDisk mapping1;
	private final LongArrayDisk mappingType1;
	private final LongArrayDisk mapping2;
	private final LongArrayDisk mappingType2;

	public CatMappingBack(String location, long size) {
		this.size = size + 1;
		this.mapping1 = new LongArrayDisk(location + "mapping_back_1", this.size);
		this.mapping2 = new LongArrayDisk(location + "mapping_back_2", this.size);
		this.mappingType1 = new LongArrayDisk(location + "mapping_back_type_1", this.size);
		this.mappingType2 = new LongArrayDisk(location + "mapping_back_type_2", this.size);
	}

	public long size() {
		return size;
	}

	public ArrayList<Long> getMapping(long i) {
		ArrayList<Long> r = new ArrayList<>();
		if (mapping1.get(i) != 0) {
			r.add(mapping1.get(i));
		}
		if (mapping2.get(i) != 0) {
			r.add(mapping2.get(i));
		}
		return r;
	}

	public ArrayList<Integer> getType(long i) {
		ArrayList<Integer> r = new ArrayList<>();
		if (mapping1.get(i) != 0) {
			r.add((int) mappingType1.get(i));
		}
		if (mapping2.get(i) != 0) {
			r.add((int) mappingType2.get(i));
		}
		return r;
	}

	public void set(long i, int mapping, int type) {
		if (this.mapping1.get(i) == 0) {
			mapping1.set(i, mapping);
			mappingType1.set(i, type);
		} else {
			mapping2.set(i, mapping);
			mappingType2.set(i, type);
		}
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(mapping1, mapping2, mappingType1, mappingType2);
	}
}

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
import com.the_qa_company.qendpoint.core.util.string.ByteString;

import java.io.Closeable;
import java.io.IOException;

public class CatMapping implements Closeable {
	private final LongArrayDisk mapping;
	private final LongArrayDisk mappingType;
	private final long size;

	public CatMapping(String location, ByteString section, long size) {
		this.size = size;
		this.mapping = new LongArrayDisk(location + section, size);
		this.mappingType = new LongArrayDisk(location + section + "Types", size);
	}

	public long getMapping(long x) {
		return mapping.get(x);
	}

	public long getType(long x) {
		return mappingType.get(x);
	}

	public void set(long x, long mapping, int type) {
		this.mapping.set(x, mapping);
		this.mappingType.set(x, type);
	}

	public long getSize() {
		return size;
	}

	public void close() throws IOException {
		IOUtil.closeAll(mapping, mappingType);
	}
}

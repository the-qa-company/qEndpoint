/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/impl/DictionaryIDMapping.java $ Revision: $Rev: 191 $ Last
 * modified: $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified
 * by: $Author: mario.arias $ This library is free software; you can
 * redistribute it and/or modify it under the terms of the GNU Lesser General
 * Public License as published by the Free Software Foundation; version 3.0 of
 * the License. This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.dictionary.impl;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mario.arias
 */
public class DictionaryIDMapping {
	class Entry {
		long newid;
		final CharSequence str;

		Entry(CharSequence str) {
			this.str = str;
			this.newid = 0;
		}
	}

	final List<Entry> list;

	public DictionaryIDMapping(long numentries) {
		if (numentries > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("This mapping class does not support more than 2G entries");
		}
		list = new ArrayList<>((int) numentries);
	}

	public void add(CharSequence str) {
		list.add(new Entry(str));
	}

	public void setNewID(long oldId, long newID) {
		list.get((int) oldId).newid = newID;
	}

	public long getNewID(long oldId) {
		// System.out.println("GetNewID old: "+oldId+"/"+list.size());
		return list.get((int) oldId).newid;
	}

	public CharSequence getString(long oldId) {
		return list.get((int) oldId).str;
	}

	public long size() {
		return list.size();
	}
}

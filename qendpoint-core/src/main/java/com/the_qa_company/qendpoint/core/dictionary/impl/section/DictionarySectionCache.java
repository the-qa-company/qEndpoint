/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/impl/section/DictionarySectionCache.java $ Revision: $Rev: 191 $
 * Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last
 * modified by: $Author: mario.arias $ This library is free software; you can
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

package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;

/**
 * DictionarySection that caches results returned by a child DictionarySection
 * to increase performance.
 *
 * @author mario.arias
 */
public class DictionarySectionCache implements DictionarySectionPrivate {

	final int CACHE_ENTRIES = 128;
	private final DictionarySectionPrivate child;

	@SuppressWarnings("serial")
	final Map<CharSequence, Long> cacheString = new LinkedHashMap<CharSequence, Long>(CACHE_ENTRIES + 1, .75F, true) {
		// This method is called just after a new entry has been added
		public boolean removeEldestEntry(Map.Entry<CharSequence, Long> eldest) {
			return size() > CACHE_ENTRIES;
		}
	};

	@SuppressWarnings("serial")
	final Map<Long, CharSequence> cacheID = new LinkedHashMap<Long, CharSequence>(CACHE_ENTRIES + 1, .75F, true) {
		// This method is called just after a new entry has been added
		public boolean removeEldestEntry(Map.Entry<Long, CharSequence> eldest) {
			return size() > CACHE_ENTRIES;
		}
	};

	public DictionarySectionCache(DictionarySectionPrivate child) {
		this.child = child;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#locate(java.lang.CharSequence)
	 */
	@Override
	public long locate(CharSequence s) {
		Long o = cacheString.get(s);
		if (o == null) {
			o = child.locate(s);
			cacheString.put(s, o);
		}
		return o;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#extract(long)
	 */
	@Override
	public CharSequence extract(long pos) {
		CharSequence o = cacheID.get(pos);
		if (o == null) {
			o = child.extract(pos);
			cacheID.put(pos, o);
			// cacheString.put(o, pos);
		}
		return o;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#size()
	 */
	@Override
	public long size() {
		return child.size();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#getNumberOfElements()
	 */
	@Override
	public long getNumberOfElements() {
		return child.getNumberOfElements();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#getEntries()
	 */
	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		return child.getSortedEntries();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#save(java.io.OutputStream,
	 * hdt.listener.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		child.save(output, listener);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#load(java.io.InputStream,
	 * hdt.listener.ProgressListener)
	 */
	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		child.load(input, listener);

	}

	/*
	 * (non-Javadoc)
	 * @see
	 * hdt.dictionary.DictionarySection#load(hdt.dictionary.DictionarySection,
	 * hdt.listener.ProgressListener)
	 */
	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		child.load(other, listener);
	}

	@Override
	public void close() throws IOException {
		child.close();
	}
}

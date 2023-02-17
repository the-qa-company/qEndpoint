/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/impl/BaseTempDictionary.java $ Revision: $Rev: 191 $ Last
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

import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TempTriples;

import java.util.Iterator;

/**
 * This abstract class implements all methods that have implementation common to
 * all modifiable dictionaries (or could apply to)
 *
 * @author Eugen
 */
public abstract class BaseTempDictionary implements TempDictionary {

	final HDTOptions spec;
	protected boolean isOrganized;

	protected TempDictionarySection subjects;
	protected TempDictionarySection predicates;
	protected TempDictionarySection objects;
	protected TempDictionarySection shared;

	public BaseTempDictionary(HDTOptions spec) {
		this.spec = spec;
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#insert(java.lang.String,
	 * datatypes.TripleComponentRole)
	 */
	@Override
	public long insert(CharSequence str, TripleComponentRole position) {
		switch (position) {
		case SUBJECT:
			isOrganized = false;
			return subjects.add(str);
		case PREDICATE:
			isOrganized = false;
			return predicates.add(str);
		case OBJECT:
			isOrganized = false;
			return objects.add(str);
		default:
			throw new IllegalArgumentException();
		}
	}

	@Override
	public void reorganize() {

		// Generate shared
		Iterator<? extends CharSequence> itSubj = subjects.getEntries();
		while (itSubj.hasNext()) {
			CharSequence str = itSubj.next();

			// FIXME: These checks really needed?
			if (str.length() > 0 && str.charAt(0) != '"' && objects.locate(str) != 0) {
				shared.add(str);
			}
		}

		// Remove shared from subjects and objects
		Iterator<? extends CharSequence> itShared = shared.getEntries();
		while (itShared.hasNext()) {
			CharSequence sharedStr = itShared.next();
			subjects.remove(sharedStr);
			objects.remove(sharedStr);
		}

		// Sort sections individually
		shared.sort();
		subjects.sort();
		objects.sort();
		predicates.sort();

		isOrganized = true;

	}

	/**
	 * This method is used in the one-pass way of working in which case it
	 * should not be used with a disk-backed dictionary because remapping
	 * requires practically a copy of the dictionary which is very bad... (it is
	 * ok for in-memory and they should override and write implementation)
	 */
	@Override
	public void reorganize(TempTriples triples) {
		throw new NotImplementedException();
	}

	@Override
	public boolean isOrganized() {
		return isOrganized;
	}

	@Override
	public void clear() {
		subjects.clear();
		predicates.clear();
		shared.clear();
		objects.clear();
	}

	@Override
	public TempDictionarySection getSubjects() {
		return subjects;
	}

	@Override
	public TempDictionarySection getPredicates() {
		return predicates;
	}

	@Override
	public TempDictionarySection getObjects() {
		return objects;
	}

	@Override
	public TempDictionarySection getShared() {
		return shared;
	}

	protected long getGlobalId(long id, DictionarySectionRole position) {
		switch (position) {
		case SUBJECT:
		case OBJECT:
			return shared.getNumberOfElements() + id;

		case PREDICATE:
		case SHARED:
			return id;
		default:
			throw new IllegalArgumentException();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#stringToId(java.lang.CharSequence,
	 * datatypes.TripleComponentRole)
	 */
	@Override
	public long stringToId(CharSequence str, TripleComponentRole position) {

		if (str == null || str.length() == 0) {
			return 0;
		}

		long ret = 0;
		switch (position) {
		case SUBJECT:
			ret = shared.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.SHARED);
			}
			ret = subjects.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.SUBJECT);
			}
			return -1;
		case PREDICATE:
			ret = predicates.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.PREDICATE);
			}
			return -1;
		case OBJECT:
			ret = shared.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.SHARED);
			}
			ret = objects.locate(str);
			if (ret != 0) {
				return getGlobalId(ret, DictionarySectionRole.OBJECT);
			}
			return -1;
		default:
			throw new IllegalArgumentException();
		}
	}
}

/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/impl/BaseDictionary.java $ Revision: $Rev: 191 $ Last modified:
 * $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by:
 * $Author: mario.arias $ This library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; version 3.0 of the License. This
 * library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 * Contacting the authors: Mario Arias: mario.arias@deri.org Javier D.
 * Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.dictionary.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryPrivate;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.enums.DictionarySectionRole;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.iterator.utils.CatIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.DelayedString;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * This abstract class implements all general methods that are the same for
 * every implementation of Dictionary.
 *
 * @author mario.arias, Eugen
 */
public abstract class BaseDictionary implements DictionaryPrivate {

	protected final HDTOptions spec;

	protected DictionarySectionPrivate subjects;
	protected DictionarySectionPrivate predicates;
	protected DictionarySectionPrivate objects;
	protected DictionarySectionPrivate shared;

	public BaseDictionary(HDTOptions spec) {
		this.spec = spec;
	}

	protected long getGlobalId(long id, DictionarySectionRole position) {
		return switch (position) {
			case SUBJECT, OBJECT -> shared.getNumberOfElements() + id;
			case PREDICATE, SHARED -> id;
			default -> throw new IllegalArgumentException();
		};
	}

	protected long getLocalId(long id, TripleComponentRole position) {
		switch (position) {
			case SUBJECT, OBJECT -> {
				if (id <= shared.getNumberOfElements()) {
					return id;
				} else {
					return id - shared.getNumberOfElements();
				}
			}
			case PREDICATE -> {
				return id;
			}
			default -> throw new IllegalArgumentException();
		}
	}

	@Override
	public Iterator<? extends CharSequence> stringIterator(TripleComponentRole role, boolean includeShared) {
		switch (role) {
			case SUBJECT -> {
				if (!includeShared) {
					return getSubjects().getSortedEntries();
				}

				return CatIterator.of(getShared().getSortedEntries(), getSubjects().getSortedEntries());
			}
			case PREDICATE -> {
				return getPredicates().getSortedEntries();
			}
			case OBJECT -> {
				if (!includeShared) {
					return getObjects().getSortedEntries();
				}

				return CatIterator.of(getShared().getSortedEntries(), getObjects().getSortedEntries());
			}
			default -> throw new IllegalArgumentException("Unknown role: " + role);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#stringToId(java.lang.CharSequence,
	 * datatypes.TripleComponentRole)
	 */
	@Override
	public long stringToId(CharSequence str, TripleComponentRole position) {
		str = DelayedString.unwrap(str);

		if (str == null || str.length() == 0) {
			return 0;
		}

		if (str instanceof String) {
			// CompactString is more efficient for the binary search.
			str = new CompactString(str);
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
			if (str.charAt(0) != '"') {
				ret = shared.locate(str);
				if (ret != 0) {
					return getGlobalId(ret, DictionarySectionRole.SHARED);
				}
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

	@Override
	public long getNumberOfElements() {
		return subjects.getNumberOfElements() + predicates.getNumberOfElements() + objects.getNumberOfElements()
				+ shared.getNumberOfElements();
	}

	@Override
	public long size() {
		return subjects.size() + predicates.size() + objects.size() + shared.size();
	}

	@Override
	public long getNsubjects() {
		return subjects.getNumberOfElements() + shared.getNumberOfElements();
	}

	@Override
	public long getNpredicates() {
		return predicates.getNumberOfElements();
	}

	@Override
	public long getNobjects() {
		return objects.getNumberOfElements() + shared.getNumberOfElements();
	}

	@Override
	public long getNshared() {
		return shared.getNumberOfElements();
	}

	@Override
	public DictionarySection getSubjects() {
		return subjects;
	}

	@Override
	public DictionarySection getPredicates() {
		return predicates;
	}

	@Override
	public DictionarySection getObjects() {
		return objects;
	}

	@Override
	public DictionarySection getShared() {
		return shared;
	}

	private DictionarySectionPrivate getSection(long id, TripleComponentRole role) {
		switch (role) {
		case SUBJECT:
			if (id <= shared.getNumberOfElements()) {
				return shared;
			} else {
				return subjects;
			}
		case PREDICATE:
			return predicates;
		case OBJECT:
			if (id <= shared.getNumberOfElements()) {
				return shared;
			} else {
				return objects;
			}
		default:
			throw new IllegalArgumentException();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#idToString(int,
	 * datatypes.TripleComponentRole)
	 */
	@Override
	public CharSequence idToString(long id, TripleComponentRole role) {
		DictionarySectionPrivate section = getSection(id, role);
		long localId = getLocalId(id, role);
		return section.extract(localId);
	}

	@Override
	public CharSequence dataTypeOfId(long id) {
		throw new IllegalArgumentException("Method is not applicable on this dictionary");
	}

	@Override
	public TreeMap<? extends CharSequence, DictionarySection> getAllObjects() {
		return new TreeMap<>(Map.of(LiteralsUtils.NO_DATATYPE, objects));
	}

	@Override
	public long getNAllObjects() {
		throw new IllegalArgumentException("Method is not applicable on this dictionary");
	}

	@Override
	public void loadAsync(TempDictionary other, ProgressListener listener) throws InterruptedException {
		throw new NotImplementedException();
	}
}

/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/impl/HashDictionary.java $ Revision: $Rev: 191 $ Last modified:
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

import java.io.IOException;
import java.util.Iterator;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryType;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.HashDictionarySection;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.util.StopWatch;

public class HashQuadDictionary extends QuadTempDictionary {

	public HashQuadDictionary(HDTOptions spec) {
		super(spec);
		// FIXME: Read types from spec
		subjects = new HashDictionarySection();
		predicates = new HashDictionarySection();
		objects = new HashDictionarySection(DictionaryType.fromDictionaryType(spec));
		shared = new HashDictionarySection();
		graphs = new HashDictionarySection();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#reorganize(hdt.triples.TempTriples)
	 */
	@Override
	public void reorganize(TempTriples triples) {
		DictionaryIDMapping mapSubj = new DictionaryIDMapping(subjects.getNumberOfElements());
		DictionaryIDMapping mapPred = new DictionaryIDMapping(predicates.getNumberOfElements());
		DictionaryIDMapping mapObj = new DictionaryIDMapping(objects.getNumberOfElements());
		DictionaryIDMapping mapGraph = new DictionaryIDMapping(graphs.getNumberOfElements());

		StopWatch st = new StopWatch();

		// Generate old subject mapping
		Iterator<? extends CharSequence> itSubj = subjects.getEntries();
		while (itSubj.hasNext()) {
			CharSequence str = itSubj.next();
			mapSubj.add(str);

			// GENERATE SHARED at the same time
			if (str.length() > 0 && str.charAt(0) != '"' && objects.locate(str) != 0) {
				shared.add(str);
			}
		}

		// Generate old predicate mapping
		st.reset();
		Iterator<? extends CharSequence> itPred = predicates.getEntries();
		while (itPred.hasNext()) {
			CharSequence str = itPred.next();
			mapPred.add(str);
		}

		// Generate old graph mapping
		Iterator<? extends CharSequence> itGraph = graphs.getEntries();
		while (itGraph.hasNext()) {
			CharSequence str = itGraph.next();
			mapGraph.add(str);
		}

		// Generate old object mapping
		Iterator<? extends CharSequence> itObj = objects.getEntries();
		while (itObj.hasNext()) {
			CharSequence str = itObj.next();
			mapObj.add(str);
		}

		// Remove shared from subjects and objects
		Iterator<? extends CharSequence> itShared = shared.getEntries();
		while (itShared.hasNext()) {
			CharSequence sharedStr = itShared.next();
			subjects.remove(sharedStr);
			objects.remove(sharedStr);
		}

		// Sort sections individually
		st.reset();
		subjects.sort();
		predicates.sort();
		graphs.sort();
		objects.sort();
		shared.sort();
		// System.out.println("Sections sorted in "+ st.stopAndShow());

		// Update mappings with new IDs
		st.reset();
		for (long j = 0; j < mapSubj.size(); j++) {
			mapSubj.setNewID(j, this.stringToId(mapSubj.getString(j), TripleComponentRole.SUBJECT));
		}

		for (long j = 0; j < mapPred.size(); j++) {
			mapPred.setNewID(j, this.stringToId(mapPred.getString(j), TripleComponentRole.PREDICATE));
		}

		for (long j = 0; j < mapObj.size(); j++) {
			mapObj.setNewID(j, this.stringToId(mapObj.getString(j), TripleComponentRole.OBJECT));
		}

		for (long j = 0; j < mapGraph.size(); j++) {
			CharSequence str = mapGraph.getString(j);
			// check that because stringToId returns 0 for empty strings and if
			// a string is empty its id is
			// 0 no matter what
			if (str.length() == 0) {
				mapGraph.setNewID(j, 1);
			} else {
				mapGraph.setNewID(j, this.stringToId(str, TripleComponentRole.GRAPH));
			}
		}

		// Replace old IDs with news
		triples.replaceAllIds(mapSubj, mapPred, mapObj, mapGraph);

		isOrganized = true;
	}

	@Override
	public void startProcessing() {
		// Do nothing.
	}

	@Override
	public void endProcessing() {
		// Do nothing.
	}

	@Override
	public void close() throws IOException {
		// Do nothing.
	}

	@Override
	public boolean supportGraphs() {
		return true;
	}
}

/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/hdt/
 * impl/TempHDTImpl.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
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

package com.the_qa_company.qendpoint.core.hdt.impl;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryFactory;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.TempHDT;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.header.HeaderFactory;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TriplesFactory;

import java.io.IOException;

/**
 * @author mario.arias, Eugen
 */
public class TempHDTImpl implements TempHDT {

	protected final Header header;
	protected final TempDictionary dictionary;
	protected final TempTriples triples;

	protected final String baseUri;

	protected ModeOfLoading modeOfLoading;
	protected boolean isOrganized;
	protected long rawsize;

	public TempHDTImpl(HDTOptions spec, String baseUri, ModeOfLoading modeOfLoading) {

		this.baseUri = baseUri;
		this.modeOfLoading = modeOfLoading;

		this.header = HeaderFactory.createHeader(spec);
		this.dictionary = DictionaryFactory.createTempDictionary(spec);
		this.triples = TriplesFactory.createTempTriples(spec);
	}

	@Override
	public Header getHeader() {
		return header;
	}

	@Override
	public TempDictionary getDictionary() {
		return dictionary;
	}

	@Override
	public TempTriples getTriples() {
		return triples;
	}

	@Override
	public void insert(CharSequence subject, CharSequence predicate, CharSequence object) {
		rawsize += subject.length() + predicate.length() + object.length() + 4;
		this.triples.insert(dictionary.insert(subject, TripleComponentRole.SUBJECT),
				dictionary.insert(predicate, TripleComponentRole.PREDICATE),
				dictionary.insert(object, TripleComponentRole.OBJECT));
		isOrganized = false;
		modeOfLoading = null;
	}

	@Override
	public void clear() {
		dictionary.clear();
		triples.clear();

		isOrganized = false;
	}

	@Override
	public void close() throws IOException {
		dictionary.close();
		triples.close();
	}

	@Override
	public String getBaseURI() {
		return baseUri;
	}

	@Override
	public void reorganizeDictionary(ProgressListener listener) {
		if (isOrganized || dictionary.isOrganized())
			return;

		// Reorganize dictionary
//		StopWatch reorgStp = new StopWatch();
		if (ModeOfLoading.ONE_PASS.equals(modeOfLoading)) {
			dictionary.reorganize(triples);
		} else if (ModeOfLoading.TWO_PASS.equals(modeOfLoading)) {
			dictionary.reorganize();
		} else if (modeOfLoading == null) {
			dictionary.reorganize(triples);
		}
		// System.out.println("Dictionary reorganized in
		// "+reorgStp.stopAndShow());
	}

	@Override
	public void reorganizeTriples(ProgressListener listener) {
		if (isOrganized)
			return;

		if (!dictionary.isOrganized())
			throw new RuntimeException("Cannot reorganize triples before dictionary is reorganized!");

		// Sort and remove duplicates.
		// StopWatch sortDupTime = new StopWatch();
		triples.sort(listener);
		triples.removeDuplicates(listener);
		// System.out.println("Sort triples and remove duplicates:
		// "+sortDupTime.stopAndShow());

		isOrganized = true;
	}

	@Override
	public boolean isOrganized() {
		return isOrganized;
	}

	public long getRawSize() {
		return rawsize;
	}
}

/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/hdt/
 * impl/TempHDTImporterTwoPass.java $ Revision: $Rev: 191 $ Last modified:
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

package com.the_qa_company.qendpoint.core.hdt.impl;

import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.TempHDT;
import com.the_qa_company.qendpoint.core.hdt.TempHDTImporter;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback.RDFCallback;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;

public class TempHDTImporterTwoPass implements TempHDTImporter {

	static class DictionaryAppender implements RDFCallback {

		final TempDictionary dict;
		final ProgressListener listener;
		long count;

		DictionaryAppender(TempDictionary dict, ProgressListener listener) {
			this.dict = dict;
			this.listener = listener;
		}

		@Override
		public void processTriple(TripleString triple, long pos) {
			dict.insert(triple.getSubject(), TripleComponentRole.SUBJECT);
			dict.insert(triple.getPredicate(), TripleComponentRole.PREDICATE);
			dict.insert(triple.getObject(), TripleComponentRole.OBJECT);
			count++;
			ListenerUtil.notifyCond(listener, "Generating dictionary " + count + " triples processed.", count, 0, 100);
		}

		public long getCount() {
			return count;
		}
	}

	/**
	 * Warning: different from HDTConverterOnePass$TripleAppender This one uses
	 * dict.stringToID, the other uses dict.insert
	 *
	 * @author mario.arias
	 */
	static class TripleAppender2 implements RDFCallback {
		final TempDictionary dict;
		final TempTriples triples;
		final ProgressListener listener;
		long count;

		public TripleAppender2(TempDictionary dict, TempTriples triples, ProgressListener listener) {
			this.dict = dict;
			this.triples = triples;
			this.listener = listener;
		}

		@Override
		public void processTriple(TripleString triple, long pos) {
			triples.insert(dict.stringToId(triple.getSubject(), TripleComponentRole.SUBJECT),
					dict.stringToId(triple.getPredicate(), TripleComponentRole.PREDICATE),
					dict.stringToId(triple.getObject(), TripleComponentRole.OBJECT));
			count++;
			ListenerUtil.notifyCond(listener, "Generating triples " + count + " triples processed.", count, 0, 100);
		}
	}

	private final HDTOptions spec;

	public TempHDTImporterTwoPass(HDTOptions spec) {
		this.spec = spec;
	}

	@Override
	public TempHDT loadFromRDF(HDTOptions specs, String filename, String baseUri, RDFNotation notation,
			ProgressListener listener) throws ParserException {

		RDFParserCallback parser = notation.createCallback(spec);

		// Create Modifiable Instance and parser
		TempHDT modHDT = new TempHDTImpl(specs, baseUri, ModeOfLoading.TWO_PASS);
		TempDictionary dictionary = modHDT.getDictionary();
		TempTriples triples = modHDT.getTriples();

		// Load RDF in the dictionary
		dictionary.startProcessing();
		parser.doParse(filename, baseUri, notation, true, new DictionaryAppender(dictionary, listener));
		dictionary.endProcessing();

		// Reorganize IDs before loading triples
		modHDT.reorganizeDictionary(listener);

		// Load triples (second pass)
		parser.doParse(filename, baseUri, notation, true, new TripleAppender2(dictionary, triples, listener));

		// reorganize HDT
		modHDT.reorganizeTriples(listener);

		return modHDT;
	}
}

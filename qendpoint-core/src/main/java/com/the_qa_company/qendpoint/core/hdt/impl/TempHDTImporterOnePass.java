/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/hdt/
 * impl/TempHDTImporterOnePass.java $ Revision: $Rev: 191 $ Last modified:
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
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.hdt.TempHDT;
import com.the_qa_company.qendpoint.core.hdt.TempHDTImporter;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback.RDFCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;

import java.util.Iterator;

public class TempHDTImporterOnePass implements TempHDTImporter {

	static class TripleAppender implements RDFCallback {
		final TempDictionary dict;
		final TempTriples triples;
		final ProgressListener listener;
		long num;
		long size;

		public TripleAppender(TempDictionary dict, TempTriples triples, ProgressListener listener) {
			this.dict = dict;
			this.triples = triples;
			this.listener = listener;
		}

		@Override
		public void processTriple(TripleString triple, long pos) {
			triples.insert(dict.insert(triple.getSubject(), TripleComponentRole.SUBJECT),
					dict.insert(triple.getPredicate(), TripleComponentRole.PREDICATE),
					dict.insert(triple.getObject(), TripleComponentRole.OBJECT));
			num++;
			size += triple.getSubject().length() + triple.getPredicate().length() + triple.getObject().length() + 4; // Spaces
																														// and
																														// final
																														// dot
			ListenerUtil.notifyCond(listener, "Loaded " + num + " triples", num, 0, 100);
		}
	}

	private final HDTOptions spec;

	public TempHDTImporterOnePass(HDTOptions spec) {
		this.spec = spec;
	}

	@Override
	public TempHDT loadFromRDF(HDTOptions specs, String filename, String baseUri, RDFNotation notation,
			ProgressListener listener) throws ParserException {

		RDFParserCallback parser = RDFParserFactory.getParserCallback(notation, spec);

		// Create Modifiable Instance
		TempHDT modHDT = new TempHDTImpl(specs, baseUri, ModeOfLoading.ONE_PASS);
		TempDictionary dictionary = modHDT.getDictionary();
		TempTriples triples = modHDT.getTriples();
		TripleAppender appender = new TripleAppender(dictionary, triples, listener);

		// Load RDF in the dictionary and generate triples
		dictionary.startProcessing();
		parser.doParse(filename, baseUri, notation, true, appender);
		dictionary.endProcessing();

		// Reorganize both the dictionary and the triples
		modHDT.reorganizeDictionary(listener);
		modHDT.reorganizeTriples(listener);

		modHDT.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, appender.size);

		return modHDT;
	}

	public TempHDT loadFromTriples(HDTOptions specs, Iterator<TripleString> iterator, String baseUri,
			ProgressListener listener) {

		// Create Modifiable Instance
		TempHDT modHDT = new TempHDTImpl(specs, baseUri, ModeOfLoading.ONE_PASS);
		TempDictionary dictionary = modHDT.getDictionary();
		TempTriples triples = modHDT.getTriples();

		// Load RDF in the dictionary and generate triples
		dictionary.startProcessing();
		long num = 0;
		long size = 0;
		while (iterator.hasNext()) {
			TripleString triple = iterator.next();
			triples.insert(dictionary.insert(triple.getSubject(), TripleComponentRole.SUBJECT),
					dictionary.insert(triple.getPredicate(), TripleComponentRole.PREDICATE),
					dictionary.insert(triple.getObject(), TripleComponentRole.OBJECT));
			num++;
			size += triple.getSubject().length() + triple.getPredicate().length() + triple.getObject().length() + 4; // Spaces
																														// and
																														// final
																														// dot
			ListenerUtil.notifyCond(listener, "Loaded " + num + " triples", num, 0, 100);
		}
		dictionary.endProcessing();

		// Reorganize both the dictionary and the triples
		modHDT.reorganizeDictionary(listener);
		modHDT.reorganizeTriples(listener);

		modHDT.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, size);

		return modHDT;
	}
}

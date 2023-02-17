/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/impl/FourSectionDictionary.java $ Revision: $Rev: 191 $ Last
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

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.DictionarySectionFactory;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySectionBig;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionThread;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author mario.arias,Lyudmila Balakireva
 */
public class FourSectionDictionaryBig extends BaseDictionary {

	public FourSectionDictionaryBig(HDTOptions spec, DictionarySectionPrivate s, DictionarySectionPrivate p,
			DictionarySectionPrivate o, DictionarySectionPrivate sh) {
		super(spec);
		this.subjects = s;
		this.predicates = p;
		this.objects = o;
		this.shared = sh;
	}

	public FourSectionDictionaryBig(HDTOptions spec) {
		super(spec);
		// FIXME: Read type from spec.
		subjects = new PFCDictionarySectionBig(spec);
		predicates = new PFCDictionarySectionBig(spec);
		objects = new PFCDictionarySectionBig(spec);
		shared = new PFCDictionarySectionBig(spec);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#load(hdt.dictionary.Dictionary)
	 */
	@Override
	public void load(TempDictionary other, ProgressListener listener) {
		IntermediateListener iListener = new IntermediateListener(listener);
		subjects.load(other.getSubjects(), iListener);
		predicates.load(other.getPredicates(), iListener);
		objects.load(other.getObjects(), iListener);
		shared.load(other.getShared(), iListener);
	}

	@Override
	public void loadAsync(TempDictionary other, ProgressListener listener) throws InterruptedException {
		IntermediateListener iListener = new IntermediateListener(null);
		new ExceptionThread(() -> predicates.load(other.getPredicates(), iListener), "FourSecSAsyncReaderP")
				.attach(new ExceptionThread(() -> subjects.load(other.getSubjects(), iListener),
						"FourSecSAsyncReaderS"),
						new ExceptionThread(() -> shared.load(other.getShared(), iListener), "FourSecSAsyncReaderSh"),
						new ExceptionThread(() -> objects.load(other.getObjects(), iListener), "FourSecSAsyncReaderO"))
				.startAll().joinAndCrashIfRequired();
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#save(java.io.OutputStream,
	 * hdt.ControlInformation, hdt.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		ci.setType(ControlInfo.Type.DICTIONARY);
		ci.setFormat(HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION);
		ci.setInt("elements", this.getNumberOfElements());
		ci.save(output);

		IntermediateListener iListener = new IntermediateListener(listener);
		shared.save(output, iListener);
		subjects.save(output, iListener);
		predicates.save(output, iListener);
		objects.save(output, iListener);

	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#load(java.io.InputStream)
	 */
	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		if (ci.getType() != ControlInfo.Type.DICTIONARY) {
			throw new IllegalFormatException("Trying to read a dictionary section, but was not dictionary.");
		}

		IntermediateListener iListener = new IntermediateListener(listener);

		shared = DictionarySectionFactory.loadFrom(input, iListener);
		subjects = DictionarySectionFactory.loadFrom(input, iListener);
		predicates = DictionarySectionFactory.loadFrom(input, iListener);
		objects = DictionarySectionFactory.loadFrom(input, iListener);
	}

	@Override
	public void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException {
		ControlInformation ci = new ControlInformation();
		ci.load(in);
		if (ci.getType() != ControlInfo.Type.DICTIONARY) {
			throw new IllegalFormatException("Trying to read a dictionary section, but was not dictionary.");
		}

		IntermediateListener iListener = new IntermediateListener(listener);
		shared = DictionarySectionFactory.loadFrom(in, f, iListener);
		subjects = DictionarySectionFactory.loadFrom(in, f, iListener);
		predicates = DictionarySectionFactory.loadFrom(in, f, iListener);
		objects = DictionarySectionFactory.loadFrom(in, f, iListener);

		// Use cache only for predicates. Preload only up to 100K predicates.
		// FIXME: DISABLED
//		predicates = new DictionarySectionCacheAll(predicates, predicates.getNumberOfElements()<100000);
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#populateHeader(hdt.header.Header,
	 * java.lang.String)
	 */
	@Override
	public void populateHeader(Header header, String rootNode) {
		header.insert(rootNode, HDTVocabulary.DICTIONARY_TYPE, HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION);
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMSUBJECTS, getNsubjects());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMPREDICATES, getNpredicates());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMOBJECTS, getNobjects());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMSHARED, getNshared());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_MAXSUBJECTID, getMaxSubjectID());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_MAXPREDICATEID, getMaxPredicateID());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_MAXOBJECTTID, getMaxObjectID());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_SIZE_STRINGS, size());
	}

	/*
	 * (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#getType()
	 */
	@Override
	public String getType() {
		return HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION;
	}

	@Override
	public void close() throws IOException {
		shared.close();
		subjects.close();
		predicates.close();
		objects.close();
	}
}

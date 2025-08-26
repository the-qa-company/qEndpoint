/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/triples
 * /TriplesFactory.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
 * 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author: mario.arias $
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples;

import com.the_qa_company.qendpoint.core.dictionary.DictionaryFactory;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapQuadTriples;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.StreamTriples;
import com.the_qa_company.qendpoint.core.triples.impl.TriplesList;
import com.the_qa_company.qendpoint.core.triples.impl.WriteBitmapTriples;
import com.the_qa_company.qendpoint.core.triples.impl.WriteStreamTriples;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;

import java.io.IOException;

/**
 * Factory that creates Triples objects
 */
public class TriplesFactory {

	private TriplesFactory() {
	}

	/**
	 * Creates a new TempTriples (writable triples structure)
	 *
	 * @return TempTriples
	 */
	static public TempTriples createTempTriples(HDTOptions spec) {
		return new TriplesList(spec);
	}

	/**
	 * Creates a new Triples based on an HDTOptions
	 *
	 * @param spec The HDTOptions to read
	 * @return Triples
	 */
	static public TriplesPrivate createTriples(HDTOptions spec) throws IOException {
		String type = spec.get("triples.format");

		if (type == null) {
			if (DictionaryFactory.isQuadDictionary(spec.get(HDTOptionsKeys.DICTIONARY_TYPE_KEY, ""))) {
				return new BitmapQuadTriples(spec);
			}
			return new BitmapTriples(spec);
		} else if (HDTVocabulary.TRIPLES_TYPE_TRIPLESLIST.equals(type)) {
			return new TriplesList(spec);
		} else if (HDTVocabulary.TRIPLES_TYPE_BITMAP.equals(type)) {
			return new BitmapTriples(spec);
		} else if (HDTVocabulary.TRIPLES_TYPE_BITMAP_QUAD.equals(type)) {
			return new BitmapQuadTriples(spec);
		} else {
			return new BitmapTriples(spec);
		}
	}

	/**
	 * Creates a new Triples based on a ControlInformation
	 *
	 * @param ci The ControlInfo to read
	 * @return Triples
	 */
	public static TriplesPrivate createTriples(ControlInfo ci) throws IOException {
		String format = ci.getFormat();

		if (HDTVocabulary.TRIPLES_TYPE_TRIPLESLIST.equals(format)) {
			return new TriplesList(new HDTSpecification());
		} else if (HDTVocabulary.TRIPLES_TYPE_BITMAP.equals(format)) {
			return new BitmapTriples();
		} else if (HDTVocabulary.TRIPLES_TYPE_BITMAP_QUAD.equals(format)) {
			return new BitmapQuadTriples();
		} else if (HDTVocabulary.TRIPLES_TYPE_STREAM.equals(format)) {
			return new StreamTriples();
		} else {
			throw new IllegalArgumentException("No implementation for Triples type: " + format);
		}
	}

	public static TriplesPrivate createWriteTriples(HDTOptions spec, CloseSuppressPath triples, int bufferSize)
			throws IOException {
		return createWriteTriples(spec, triples, bufferSize, -1);
	}

	public static TriplesPrivate createWriteTriples(HDTOptions spec, CloseSuppressPath triples, int bufferSize,
			long quads) throws IOException {
		String format = spec.get(HDTOptionsKeys.DISK_WRITE_TRIPLES_TYPE_KEY,
				HDTOptionsKeys.DISK_WRITE_TRIPLES_TYPE_VALUE_BITMAP);

		switch (format) {
		case HDTOptionsKeys.DISK_WRITE_TRIPLES_TYPE_VALUE_BITMAP -> {
			return new WriteBitmapTriples(spec, triples, bufferSize, quads);
		}
		case HDTOptionsKeys.DISK_WRITE_TRIPLES_TYPE_VALUE_STREAM -> {
			return new WriteStreamTriples(spec, triples, bufferSize, quads);
		}
		default -> throw new IllegalArgumentException("No implementation for write triples type: " + format);
		}
	}

}

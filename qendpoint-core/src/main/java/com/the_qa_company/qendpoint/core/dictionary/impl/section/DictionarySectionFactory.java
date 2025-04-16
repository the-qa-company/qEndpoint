/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/
 * dictionary/impl/section/DictionarySectionFactory.java $ Revision: $Rev: 194 $
 * Last modified: $Date: 2013-03-04 21:30:01 +0000 (lun, 04 mar 2013) $ Last
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;

/**
 * @author mario.arias
 */
public class DictionarySectionFactory {

	private DictionarySectionFactory() {
	}

	public static DictionarySectionPrivate createWriteSection(HDTOptions spec, Path filename, int bufferSize) {
		String type = spec.get(HDTOptionsKeys.DISK_WRITE_SECTION_TYPE_KEY,
				HDTOptionsKeys.DISK_WRITE_SECTION_TYPE_VALUE_PFC);
		return switch (type) {
		case HDTOptionsKeys.DISK_WRITE_SECTION_TYPE_VALUE_PFC -> new WriteDictionarySection(spec, filename, bufferSize);
		case HDTOptionsKeys.DISK_WRITE_SECTION_TYPE_VALUE_STREAM ->
			new WriteStreamDictionarySection(spec, filename, bufferSize);
		default -> throw new IllegalArgumentException("No write implementation for type " + type);
		};
	}

	public static DictionarySectionPrivate createDictionarySection(HDTOptions spec) {
		return createDictionarySection(spec, "");
	}

	public static DictionarySectionPrivate createDictionarySection(HDTOptions spec, String defaultValue) {
		String name = spec.get(HDTOptionsKeys.DICTIONARY_SECTION_TYPE_KEY, defaultValue);

		return switch (name) {
		case "", HDTOptionsKeys.DICTIONARY_SECTION_TYPE_VALUE_PFC -> new PFCDictionarySectionBig(spec);
		case HDTOptionsKeys.DICTIONARY_SECTION_TYPE_VALUE_STREAM -> new StreamDictionarySection(spec);
		default -> throw new IllegalFormatException("Implementation of dictionary section not found for " + name);
		};
	}

	public static DictionarySectionPrivate loadFrom(InputStream input, ProgressListener listener) throws IOException {
		if (!input.markSupported()) {
			throw new IllegalArgumentException(
					"Need support for mark()/reset(). Please wrap the InputStream with a BufferedInputStream");
		}
		input.mark(64);
		int dictType = input.read();
		input.reset();
		input.mark(64); // To allow children to reset() and try another
		// instance.

		DictionarySectionPrivate section;

		switch (dictType) {
		case PFCDictionarySection.TYPE_INDEX:
			section = DictionarySectionFactory.createDictionarySection(HDTOptions.of());
			section.load(input, listener);
			return section;
		case StreamDictionarySection.TYPE_INDEX:
			// First try load using the standard PFC
			section = new StreamDictionarySection(HDTOptions.of());
			section.load(input, listener);
			return section;
		default:
			throw new IOException("DictionarySection implementation not available for id " + dictType);
		}
	}

	public static DictionarySectionPrivate loadFrom(CountInputStream input, File f, ProgressListener listener)
			throws IOException {
		input.mark(64);
		int dictType = input.read();
		input.reset();
		input.mark(64); // To allow children to reset() and try another
		// instance.

		return switch (dictType) {
		case PFCDictionarySection.TYPE_INDEX -> new PFCDictionarySectionMap(input, f);
		case StreamDictionarySection.TYPE_INDEX -> new StreamDictionarySectionMap(input, f);
		default -> throw new IOException("DictionarySection implementation not available for id " + dictType);
		};
	}
}

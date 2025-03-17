/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/
 * options/ControlInfo.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License,
 * or (at your option) any later version. This library is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.options;

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author mario.arias
 */
public interface ControlInfo extends HDTOptions {
	enum Type {
		UNKNOWN, GLOBAL, HEADER, DICTIONARY, TRIPLES, INDEX, QEPCORE_MERGE
	}

	Type getType();

	void setType(Type type);

	String getFormat();

	void setFormat(String format);

	default void save(Path filename) throws IOException {
		try (OutputStream os = new FastBufferedOutputStream(Files.newOutputStream(filename))) {
			save(os);
		}
	}

	default void save(String filename) throws IOException {
		save(Path.of(filename));
	}

	void save(OutputStream output) throws IOException;

	void load(InputStream input) throws IOException;

}

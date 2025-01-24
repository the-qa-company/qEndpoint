/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/io
 * /ByteBufferInputStream.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

package com.the_qa_company.qendpoint.core.util.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author mario.arias
 */
public class ByteBufferInputStream extends InputStream {
	final ByteBuffer buf;

	public ByteBufferInputStream(ByteBuffer buf) {
		this.buf = buf;
	}

	@Override
	public synchronized int read() throws IOException {
		if (!buf.hasRemaining()) {
			return -1;
		}
		return Byte.toUnsignedInt(buf.get());
	}

	@Override
	public synchronized int read(byte[] bytes, int off, int len) throws IOException {
		if (!buf.hasRemaining()) {
			return -1;
		}
		// Read only what's left
		len = Math.min(len, buf.remaining());
		buf.get(bytes, off, len);
		return len;
	}

	@Override
	public long skip(long n) {
		buf.position(buf.position() + (int)n);
		return n;
	}

}

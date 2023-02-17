/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/options
 * /ControlInformation.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

package com.the_qa_company.qendpoint.core.options;

import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.util.crc.CRC16;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;

/**
 * @author mario.arias
 */
public class ControlInformation extends HDTOptionsBase implements ControlInfo {

	ControlInfo.Type type;
	String format;

	public ControlInformation() {
		super();
	}

	@Override
	public ControlInfo.Type getType() {
		return type;
	}

	@Override
	public void setType(ControlInfo.Type type) {
		this.type = type;
	}

	@Override
	public String getFormat() {
		return format;
	}

	@Override
	public void setFormat(String format) {
		this.format = format;
	}

	@Override
	public void save(OutputStream output) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC16());

		// Cookie
		IOUtil.writeString(out, "$HDT");

		// Type
		IOUtil.writeByte(out, (byte) type.ordinal());

		// Format
		IOUtil.writeString(out, format);
		out.write(0);

		// Properties
		for (Enumeration<Object> e = properties.keys(); e.hasMoreElements();) {
			String key = (String) e.nextElement();

			IOUtil.writeString(out, key + '=' + properties.getProperty(key) + ";");
		}
		out.write(0); // Null terminator

		// CRC
		out.writeCRC();
	}

	@Override
	public void load(InputStream input) throws IOException {
		CRCInputStream in = new CRCInputStream(input, new CRC16());

		// Cookie
		String magic = IOUtil.readChars(in, 4);
		if (!magic.equals("$HDT")) {
			throw new IOException("Non-HDT Section");
		}

		// Type
		try {
			type = Type.values()[IOUtil.readByte(in)];
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new IllegalFormatException("The type of the ControlInformation is unknown for this implementation");
		}

		// Format
		format = IOUtil.readLine(in, '\0');

		// Properties
		String propertiesStr = IOUtil.readLine(in, '\0');
		for (String item : propertiesStr.split(";")) {
			int pos = item.indexOf('=');
			if (pos != -1) {
				String property = item.substring(0, pos);
				String value = item.substring(pos + 1);
				properties.put(property, value);
			}
		}

		// CRC
		in.readCRCAndCheck();
	}

	@Override
	public void clear() {
		type = Type.UNKNOWN;
		format = null;
		super.clear();
	}
}

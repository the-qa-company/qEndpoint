/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact
 * /bitmap/BitmapFactory.java $ Revision: $Rev: 191 $ Last modified: $Date:
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

package com.the_qa_company.qendpoint.core.compact.bitmap;

import java.io.IOException;
import java.io.InputStream;

import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;

/**
 * @author mario.arias
 */
public class BitmapFactoryImpl extends BitmapFactory {
	private final Bitmap EMPTY = Bitmap64Big.memory(0);

	@Override
	protected ModifiableBitmap doCreateModifiableBitmap(String type) {
		if (type == null || type.equals(HDTVocabulary.BITMAP_TYPE_PLAIN)) {
			return Bitmap375Big.memory(0);
		} else {
			throw new IllegalArgumentException("Implementation not found for Bitmap with type " + type);
		}
	}

	@Override
	protected Bitmap doCreateBitmap(InputStream input) throws IOException {
		input.mark(1);
		int value = input.read();
		input.reset();
		if (value == TYPE_BITMAP_PLAIN) {
			return Bitmap375Big.memory(0);
		}
		throw new IllegalFormatException("Implementation not found for Bitmap with code " + value);
	}

	@Override
	protected ModifiableBitmap doCreateRWModifiableBitmap(long size) {
		return Bitmap64Big.memory(size);
	}

	@Override
	protected Bitmap doEmpty() {
		return EMPTY;
	}
}

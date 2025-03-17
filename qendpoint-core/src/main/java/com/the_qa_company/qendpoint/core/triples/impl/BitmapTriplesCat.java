/**
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; version 3.0 of the License. This library is distributed
 * in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Dennis
 * Diefenbach: dennis.diefenbach@univ-st-etienne.fr
 */

package com.the_qa_company.qendpoint.core.triples.impl;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap375Big;
import com.the_qa_company.qendpoint.core.compact.bitmap.ModifiableBitmap;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;

public class BitmapTriplesCat {

	private final String location;

	public BitmapTriplesCat(String location) {
		this.location = location;
	}

	public void cat(IteratorTripleID it, ProgressListener listener) throws IOException {
		long number = it.estimatedNumResults();
		SequenceLog64BigDisk vectorY = new SequenceLog64BigDisk(location + "vectorY", BitUtil.log2(number), number);
		SequenceLog64BigDisk vectorZ = new SequenceLog64BigDisk(location + "vectorZ", BitUtil.log2(number), number);
		ModifiableBitmap bitY = Bitmap375Big.memory(0);// Disk(location +
														// "bitY",number);
		ModifiableBitmap bitZ = Bitmap375Big.memory(0);// Disk(location +
														// "bitZ",number);

		long lastX = 0, lastY = 0, lastZ = 0;
		long x, y, z;
		long numTriples = 0;

		while (it.hasNext()) {
			TripleID triple = it.next();
			TripleOrderConvert.swapComponentOrder(triple, TripleComponentOrder.SPO, TripleComponentOrder.SPO);

			x = triple.getSubject();
			y = triple.getPredicate();
			z = triple.getObject();
			if (x == 0 || y == 0 || z == 0) {
				throw new IllegalFormatException("None of the components of a triple can be null");
			}

			if (numTriples == 0) {
				// First triple
				vectorY.append(y);
				vectorZ.append(z);
			} else if (x != lastX) {
				if (x != lastX + 1) {
					throw new IllegalFormatException("Upper level must be increasing and correlative.");
				}
				// X changed
				bitY.append(true);
				vectorY.append(y);

				bitZ.append(true);
				vectorZ.append(z);
			} else if (y != lastY) {
				if (y < lastY) {
					throw new IllegalFormatException("Middle level must be increasing for each parent.");
				}

				// Y changed
				bitY.append(false);
				vectorY.append(y);

				bitZ.append(true);
				vectorZ.append(z);
			} else {
				if (z < lastZ) {
					throw new IllegalFormatException("Lower level must be increasing for each parent.");
				}

				// Z changed
				bitZ.append(false);
				vectorZ.append(z);
			}

			lastX = x;
			lastY = y;
			lastZ = z;

			ListenerUtil.notifyCond(listener, "Converting to BitmapTriples", numTriples, numTriples, number);
			numTriples++;
		}

		if (numTriples > 0) {
			bitY.append(true);
			bitZ.append(true);
		}

		vectorY.aggressiveTrimToSize();
		vectorZ.trimToSize();

		try (OutputStream bos = new FastBufferedOutputStream(new FileOutputStream(location + "triples"))) {
			ControlInfo ci = new ControlInformation();
			ci.setType(ControlInfo.Type.TRIPLES);
			ci.setFormat(HDTVocabulary.TRIPLES_TYPE_BITMAP);
			ci.setInt("order", TripleComponentOrder.SPO.ordinal());
			ci.setType(ControlInfo.Type.TRIPLES);
			ci.save(bos);
			IntermediateListener iListener = new IntermediateListener(listener);
			bitY.save(bos, iListener);
			bitZ.save(bos, iListener);
			vectorY.save(bos, iListener);
			vectorZ.save(bos, iListener);
		} finally {
			IOUtil.closeAll(vectorY, vectorZ);
		}
		Files.delete(Paths.get(location + "vectorY"));
		Files.delete(Paths.get(location + "vectorZ"));
		// Files.delete(Paths.get(location + "bitY"));
		// Files.delete(Paths.get(location + "bitZ"));
	}
}

package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

/**
 * {@link ModifiableBitmap} implementation for only appending/one read saving
 * into a buffer file
 *
 * @author Antoine Willerval
 */
public class AppendableWriteBitmap implements ModifiableBitmap, Closeable {
	private long countZeros;
	private long countOnes;
	private long numbits;
	private final CloseSuppressPath file;
	private final CRCOutputStream stream;
	private long currentElement;
	private int bit;
	private boolean saved;

	public AppendableWriteBitmap(CloseSuppressPath storage, int bufferSize) throws IOException {
		file = storage;
		stream = new CRCOutputStream(storage.openOutputStream(bufferSize), new CRC32());
	}

	@Override
	public void set(long position, boolean value) {
		throw new NotImplementedException();
	}

	@Override
	public void append(boolean value) {
		// count for stats
		if (value) {
			countOnes++;
		} else {
			countZeros++;
		}
		// increase the numbits
		numbits++;

		// set the value
		if (value) {
			currentElement |= 1L << bit;
		}
		bit++;

		// write the value if required
		try {
			pushByte(false);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void pushByte(boolean force) throws IOException {
		if (bit == 64 || force) {
			BitUtil.writeLowerBitsByteAligned(currentElement, bit, stream);
			// reset the current element writing
			bit = 0;
			currentElement = 0L;
		}
	}

	@Override
	public boolean access(long position) {
		throw new NotImplementedException();
	}

	@Override
	public long rank1(long position) {
		throw new NotImplementedException();
	}

	@Override
	public long rank0(long position) {
		throw new NotImplementedException();
	}

	@Override
	public long selectPrev1(long start) {
		throw new NotImplementedException();
	}

	@Override
	public long selectNext1(long start) {
		throw new NotImplementedException();
	}

	@Override
	public long select0(long n) {
		throw new NotImplementedException();
	}

	@Override
	public long select1(long n) {
		throw new NotImplementedException();
	}

	@Override
	public long getNumBits() {
		return numbits;
	}

	@Override
	public long countOnes() {
		return countOnes;
	}

	@Override
	public long countZeros() {
		return countZeros;
	}

	@Override
	public long getSizeBytes() {
		return (numbits - 1) / 8 + 1;
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		saved = true;
		// maybe a bit was already reading
		pushByte(true);
		// complete the file
		stream.writeCRC();
		stream.close();

		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		// Write Type and Numbits
		out.write(BitmapFactory.TYPE_BITMAP_PLAIN);
		VByte.encode(out, numbits);

		// Write CRC
		out.writeCRC();

		// write the storage file, already contains the CRC
		Files.copy(file, output);

		// delete the file
		file.close();
	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public String getType() {
		return HDTVocabulary.BITMAP_TYPE_PLAIN;
	}

	@Override
	public void close() throws IOException {
		if (!saved) {
			IOUtil.closeAll(stream, file);
		}
	}
}

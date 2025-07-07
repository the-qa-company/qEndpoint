package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.BigByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.BigByteBufferInputStream;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBufferInputStream;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StreamTriples implements TriplesPrivate {
	public static final int FLAG_SAME_SUBJECT = 1;
	public static final int FLAG_SAME_PREDICATE = 1 << 1;
	public static final int FLAG_END = 1 << 2;
	public static final int FLAG_SHARED_END = 1 << 3;
	public static final int STREAM_TRIPLES_END_COOKIE = 0x48545324;
	private long numTriples;
	private long numShared;
	private long numSharedTriples;
	private long compressedSizeShared;
	private long compressedSizeCommon;
	private CompressionType compressionType = CompressionType.NONE;
	private FileChannel ch;
	private BigMappedByteBuffer mappedShared;
	private BigMappedByteBuffer mappedCommon;
	private BigByteBuffer bufferShared;
	private BigByteBuffer bufferCommon;
	private TripleComponentOrder order;

	public void cleanup() throws IOException {
		bufferShared = null;
		bufferCommon = null;
		if (mappedShared != null) {
			mappedShared.clean();
			mappedShared = null;
		}
		if (mappedCommon != null) {
			mappedCommon.clean();
			mappedCommon = null;
		}
		if (ch != null) {
			ch.close();
			ch = null;
		}
	}

	private InputStream stream(boolean shared) throws IOException {
		// ignore end CRC
		if (mappedShared != null || mappedCommon != null) {
			return shared ? new BigMappedByteBufferInputStream(mappedShared) : new BigMappedByteBufferInputStream(mappedCommon);
		}

		if (bufferShared != null || bufferCommon != null) {
			return shared ? new BigByteBufferInputStream(bufferShared) : new BigByteBufferInputStream(bufferCommon);
		}

		throw new IOException("StreamTriples not loaded");
	}

	private InputStream uncompressedStream(boolean shared) throws IOException {
		return compressionType.decompress(stream(shared));
	}

	@Override
	public void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		ci.clear();
		ci.setFormat(getType());
		ci.setInt("order", order.ordinal());
		ci.setType(ControlInfo.Type.TRIPLES);
		ci.save(output);

		IntermediateListener iListener = new IntermediateListener(listener);
		CRCOutputStream crc = new CRCOutputStream(output, new CRC8());
		VByte.encode(crc, numTriples);
		VByte.encode(crc, numShared);
		VByte.encode(crc, numSharedTriples);
		VByte.encode(crc, compressedSizeShared);
		VByte.encode(crc, compressedSizeCommon);
		IOUtil.writeSizedString(crc, compressionType.name(), iListener);
		crc.writeCRC();

		try (InputStream is = stream(true)) {
			is.transferTo(output); // the stream already has its own crc
		}
		try (InputStream is = stream(false)) {
			is.transferTo(output); // the stream already has its own crc
		}
		IOUtil.writeInt(output, STREAM_TRIPLES_END_COOKIE); // end cookie
	}

	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		if (ci.getType() != ControlInfo.Type.TRIPLES) {
			throw new IllegalFormatException("Trying to read a triples section, but was not triples.");
		}

		if (!ci.getFormat().equals(getType())) {
			throw new IllegalFormatException(
					"Trying to read BitmapTriples, but the data does not seem to be StreamTriples");
		}

		order = TripleComponentOrder.values()[(int) ci.getInt("order")];

		IntermediateListener iListener = new IntermediateListener(listener);
		CRCInputStream crc = new CRCInputStream(input, new CRC8());

		numTriples = VByte.decode(crc);
		numShared = VByte.decode(crc);
		numSharedTriples = VByte.decode(crc);
		compressedSizeShared = VByte.decode(crc);
		compressedSizeCommon = VByte.decode(crc);

		String compressionFormatName = IOUtil.readSizedString(crc, iListener);

		try {
			compressionType = CompressionType.valueOf(compressionFormatName);
		} catch (IllegalArgumentException e) {
			throw new IOException("can't find compression type implementation with name " + compressionFormatName, e);
		}

		if (!crc.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading StreamTriples Header.");
		}

		try {
			bufferShared = BigByteBuffer.allocate(compressedSizeShared);
			bufferCommon = BigByteBuffer.allocate(compressedSizeCommon);

			bufferShared.readStream(input, 0, compressedSizeShared, iListener);
			bufferCommon.readStream(input, 0, compressedSizeCommon, iListener);

			int cookie = IOUtil.readInt(input);
			if (cookie != STREAM_TRIPLES_END_COOKIE) {
				throw new IOException("Can't read stream triples end cookie, found 0x" + Integer.toHexString(cookie));
			}
		} catch (Throwable t) {
			cleanup();
			throw t;
		}
	}

	@Override
	public void mapFromFile(CountInputStream input, File f, ProgressListener listener) throws IOException {
		ControlInformation ci = new ControlInformation();
		ci.load(input);
		if (ci.getType() != ControlInfo.Type.TRIPLES) {
			throw new IllegalFormatException("Trying to read a triples section, but was not triples.");
		}

		if (!ci.getFormat().equals(getType())) {
			throw new IllegalFormatException(
					"Trying to read BitmapTriples, but the data does not seem to be StreamTriples");
		}

		order = TripleComponentOrder.values()[(int) ci.getInt("order")];

		IntermediateListener iListener = new IntermediateListener(listener);
		CRCInputStream crc = new CRCInputStream(input, new CRC8());

		numTriples = VByte.decode(crc);
		numShared = VByte.decode(crc);
		numSharedTriples = VByte.decode(crc);
		compressedSizeShared = VByte.decode(crc);
		compressedSizeCommon = VByte.decode(crc);

		String compressionFormatName = IOUtil.readSizedString(crc, iListener);

		try {
			compressionType = CompressionType.valueOf(compressionFormatName);
		} catch (IllegalArgumentException e) {
			throw new IOException("can't find compression type implementation with name " + compressionFormatName, e);
		}

		if (!crc.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading StreamTriples Header.");
		}

		try {
			ch = FileChannel.open(Paths.get(f.toString()));
			long base = input.getTotalBytes();
			mappedShared = BigMappedByteBuffer.ofFileChannel(f.getAbsolutePath(), ch, FileChannel.MapMode.READ_ONLY, base, compressedSizeShared);
			mappedCommon = BigMappedByteBuffer.ofFileChannel(f.getAbsolutePath(), ch, FileChannel.MapMode.READ_ONLY, base + compressedSizeShared, compressedSizeCommon);
			IOUtil.skip(input, compressedSizeShared + compressedSizeCommon);

			int cookie = IOUtil.readInt(input);
			if (cookie != STREAM_TRIPLES_END_COOKIE) {
				throw new IOException("Can't read stream triples end cookie, found 0x" + Integer.toHexString(cookie));
			}
		} catch (Throwable t) {
			cleanup();
			throw t;
		}
	}

	@Override
	public void generateIndex(ProgressListener listener, HDTOptions spec, Dictionary dictionary) throws IOException {
		// nothing
	}

	@Override
	public void loadIndex(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		// nothing
	}

	@Override
	public void mapIndex(CountInputStream input, File f, ControlInfo ci, ProgressListener listener) throws IOException {
		// nothing
	}

	@Override
	public void mapGenOtherIndexes(Path file, HDTOptions spec, ProgressListener listener) throws IOException {
		// nothing
	}

	@Override
	public void saveIndex(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		// nothing
	}

	@Override
	public void load(TempTriples input, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public TripleComponentOrder getOrder() {
		return order;
	}

	@Override
	public SuppliableIteratorTripleID searchAll() {
		return search(new TripleID());
	}

	@Override
	public SuppliableIteratorTripleID searchAll(int searchMask) {
		return searchAll();
	}

	@Override
	public SuppliableIteratorTripleID search(TripleID pattern) {
		if (!pattern.isEmpty()) {
			if (pattern.getSubject() != numShared + 1 || pattern.getPredicate() != 0 || pattern.getObject() != 0) {
				// we can do it by filtering the triples, but it would be too long
				throw new IllegalArgumentException("Can't search pattern over stream triples!");
			}
			return new StreamReader(false);
		}
		return new StreamReader(true);
	}

	@Override
	public SuppliableIteratorTripleID search(TripleID pattern, int searchMask) {
		return search(pattern);
	}

	@Override
	public long getNumberOfElements() {
		return numTriples;
	}

	@Override
	public long size() {
		return compressedSizeShared + compressedSizeCommon;
	}

	@Override
	public void populateHeader(Header header, String rootNode) {
		if (rootNode == null || rootNode.isEmpty()) {
			throw new IllegalArgumentException("Root node for the header cannot be null");
		}

		header.insert(rootNode, HDTVocabulary.TRIPLES_TYPE, getType());
		header.insert(rootNode, HDTVocabulary.TRIPLES_NUM_TRIPLES, getNumberOfElements());
		header.insert(rootNode, HDTVocabulary.TRIPLES_ORDER, order.toString());
	}

	@Override
	public String getType() {
		return HDTVocabulary.TRIPLES_TYPE_STREAM;
	}

	@Override
	public TripleID findTriple(long position, TripleID buffer) {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
		cleanup();
	}

	public class StreamReader implements SuppliableIteratorTripleID {
		private InputStream stream;
		private long offset;
		private final TripleID triple = new TripleID();

		private StreamReader(boolean startShared) {
			if (startShared) {
				goToStart();
			} else {
				goToAfterShared();
			}
		}

		@Override
		public boolean hasPrevious() {
			return false;
		}

		@Override
		public TripleID previous() {
			return null;
		}

		private void goToAfterShared() {
			try {
				// start at the shared
				offset = numSharedTriples;
				stream = uncompressedStream(false);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void goToStart() {
			if (numSharedTriples == 0) {
				goToAfterShared();
				triple.setAll(numShared, 0, 0);
				return;
			}
			try {
				offset = 0;
				stream = uncompressedStream(true);
				triple.setAll(0, 0, 0);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public boolean canGoTo() {
			return false;
		}

		@Override
		public void goTo(long pos) {
			if (pos == numSharedTriples) {
				goToAfterShared();
				triple.setAll(numShared, 0, 0);
				return;
			}
			if (pos == 0) {
				goToStart();
				return;
			}
			throw new NotImplementedException();
		}

		@Override
		public long estimatedNumResults() {
			return numTriples;
		}

		@Override
		public ResultEstimationType numResultEstimation() {
			return ResultEstimationType.EXACT;
		}

		@Override
		public TripleComponentOrder getOrder() {
			return order;
		}

		@Override
		public long getLastTriplePosition() {
			return offset - 1;
		}

		@Override
		public boolean hasNext() {
			return offset < numTriples;
		}

		@Override
		public TripleID next() {
			if (!hasNext()) return null;

			offset++;

			try {
				int flags = stream.read();
				if ((flags & FLAG_END) != 0) {
					throw new IOException("Found end triple");
				}

				if ((flags & FLAG_SAME_SUBJECT) == 0) {
					triple.setSubject(triple.getSubject() + 1); // increase subject id
				}

				if ((flags & FLAG_SAME_PREDICATE) == 0) {
					triple.setPredicate(VByte.decode(stream));
				}

				triple.setObject(VByte.decode(stream));

				if (offset == numSharedTriples) {
					goToAfterShared(); // we need to swap to the shared buffer
				}

				return triple;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public void checkEnd() throws IOException {
			int flags = stream.read();
			if ((flags & FLAG_END) == 0) {
				throw new IOException("No end flag");
			}
		}
	}
}

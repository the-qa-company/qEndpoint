package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.iterator.SuppliableIteratorTripleID;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TempTriples;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.triples.TriplesPrivate;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.CountOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.IntermediateListener;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class WriteStreamTriples implements TriplesPrivate {

	protected TripleComponentOrder order;
	private long numTriples;
	private long numShared;
	private long numSharedTriples;
	private long compressedSizeShared;
	private long compressedSizeCommon;
	private long decompressedSizeShared;
	private long decompressedSizeCommon;
	private final CloseSuppressPath triples;
	private final CloseSuppressPath triplesShared;
	private final CloseSuppressPath triplesCommon;
	private final CompressionType compressionType;
	private final int bufferSize;

	public WriteStreamTriples(HDTOptions spec, CloseSuppressPath triples, int bufferSize) throws IOException {
		this(spec, triples, bufferSize, -1);
	}

	public WriteStreamTriples(HDTOptions spec, CloseSuppressPath triples, int bufferSize, long quads)
			throws IOException {
		if (quads != -1)
			throw new IllegalArgumentException("stream quads not supported");
		String orderStr = spec.get(HDTOptionsKeys.TRIPLE_ORDER_KEY);
		if (orderStr == null) {
			this.order = TripleComponentOrder.SPO;
		} else {
			this.order = TripleComponentOrder.valueOf(orderStr);
		}
		triples.mkdirs();
		triples.closeWithDeleteRecurse();
		this.triples = triples;
		this.triplesCommon = triples.resolve("ctr");
		this.triplesShared = triples.resolve("str");
		this.bufferSize = bufferSize;
		compressionType = CompressionType.findOptionVal(spec.get(HDTOptionsKeys.DISK_COMPRESSION_KEY));
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
		VByte.encode(crc, decompressedSizeShared);
		VByte.encode(crc, decompressedSizeCommon);
		IOUtil.writeSizedString(crc, compressionType.name(), iListener);
		crc.writeCRC();

		assert compressedSizeShared == Files.size(triplesShared);
		assert compressedSizeCommon == Files.size(triplesCommon);
		Files.copy(this.triplesShared, output);
		Files.copy(this.triplesCommon, output);
		IOUtil.writeInt(output, StreamTriples.STREAM_TRIPLES_END_COOKIE); // end
																			// cookie
	}

	@Override
	public IteratorTripleID searchAll() {
		throw new NotImplementedException();
	}

	@Override
	public IteratorTripleID searchAll(int searchMask) {
		throw new NotImplementedException();
	}

	@Override
	public SuppliableIteratorTripleID search(TripleID pattern) {
		throw new NotImplementedException();
	}

	@Override
	public SuppliableIteratorTripleID search(TripleID pattern, int searchMask) {
		throw new NotImplementedException();
	}

	@Override
	public long getNumberOfElements() {
		return numTriples;
	}

	@Override
	public long size() {
		return numTriples * 4;
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
		// return quadInfoAG != null ? HDTVocabulary.TRIPLES_TYPE_STREAM_QUAD :
		// HDTVocabulary.TRIPLES_TYPE_STREAM;
		return HDTVocabulary.TRIPLES_TYPE_STREAM;
	}

	@Override
	public TripleID findTriple(long position, TripleID tripleID) {
		throw new NotImplementedException();
	}

	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void mapFromFile(CountInputStream in, File f, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void generateIndex(ProgressListener listener, HDTOptions disk, Dictionary dictionary) {
		throw new NotImplementedException();
	}

	@Override
	public void loadIndex(InputStream input, ControlInfo ci, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void mapIndex(CountInputStream input, File f, ControlInfo ci, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void mapGenOtherIndexes(Path file, HDTOptions spec, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void saveIndex(OutputStream output, ControlInfo ci, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void load(TempTriples triples, ProgressListener listener) {
		triples.setOrder(order);
		triples.sort(listener);

		IteratorTripleID itid = triples.searchAll();

		long number = itid.estimatedNumResults();
		PeekIterator<TripleID> it = PeekIterator.of(itid);
		numTriples = 0;
		compressedSizeShared = 0;
		compressedSizeCommon = 0;
		decompressedSizeShared = 0;
		decompressedSizeCommon = 0;
		numShared = triples.getSharedCount();
		numSharedTriples = 0;
		try {
			if (numShared != 0) {
				// start compress
				CountOutputStream compressedStream = new CountOutputStream(
						this.triplesShared.openOutputStream(bufferSize));
				CRCOutputStream crcout = new CRCOutputStream(
						new BufferedOutputStream(compressionType.compress(compressedStream)), new CRC32());
				try (CountOutputStream out = new CountOutputStream(crcout)) {
					long lastSubject = 0;
					long lastPred = 0;
					for (; it.hasNext(); it.next()) {
						TripleID tid = it.peek();
						int flags = 0;
						if (lastSubject == tid.getSubject()) {
							flags |= StreamTriples.FLAG_SAME_SUBJECT;
						} else {
							if (lastSubject + 1 != tid.getSubject()) {
								throw new IllegalFormatException("Non cumulative subjects");
							}
							if (tid.getSubject() == numShared + 1) {
								break; // me need to swap to the common data
							}
							lastSubject = tid.getSubject();
						}
						if (lastPred == tid.getPredicate()) {
							flags |= StreamTriples.FLAG_SAME_PREDICATE;
						}
						out.write(flags);
						if (lastPred != tid.getPredicate()) {
							VByte.encode(out, tid.getPredicate());
						}
						VByte.encode(out, tid.getObject());
						numTriples++;

						lastPred = tid.getPredicate();

						ListenerUtil.notifyCond(listener, "Converting to StreamTriples " + numTriples + "/" + number,
								numTriples, numTriples, number);
					}
					out.write(StreamTriples.FLAG_END | StreamTriples.FLAG_SHARED_END);
					decompressedSizeShared = out.getTotalBytes();
					crcout.writeCRC();
					crcout.flush();
				}
				compressedSizeShared = compressedStream.getTotalBytes();
				numSharedTriples = numTriples;
			}
			{
				CountOutputStream compressedStream = new CountOutputStream(
						this.triplesCommon.openOutputStream(bufferSize));
				CRCOutputStream crcout = new CRCOutputStream(
						new BufferedOutputStream(compressionType.compress(compressedStream)), new CRC32());
				try (CountOutputStream out = new CountOutputStream(crcout)) {
					long lastSubject = numShared;
					long lastPred = 0;
					for (; it.hasNext(); it.next()) {
						TripleID tid = it.peek();
						int flags = 0;
						if (lastSubject == tid.getSubject()) {
							flags |= StreamTriples.FLAG_SAME_SUBJECT;
						} else {
							if (lastSubject + 1 != tid.getSubject()) {
								throw new IllegalFormatException("Non cumulative subjects");
							}
							lastSubject = tid.getSubject();
						}
						if (lastPred == tid.getPredicate()) {
							flags |= StreamTriples.FLAG_SAME_PREDICATE;
						}
						out.write(flags);
						if (lastPred != tid.getPredicate()) {
							VByte.encode(out, tid.getPredicate());
						}
						VByte.encode(out, tid.getObject());
						numTriples++;

						lastPred = tid.getPredicate();

						ListenerUtil.notifyCond(listener, "Converting to StreamTriples " + numTriples + "/" + number,
								numTriples, numTriples, number);
					}
					out.write(StreamTriples.FLAG_END);
					decompressedSizeCommon = out.getTotalBytes();
					crcout.writeCRC();
					crcout.flush();
				}
				compressedSizeCommon = compressedStream.getTotalBytes();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public TripleComponentOrder getOrder() {
		return order;
	}

	@Override
	public void close() throws IOException {
		Closer.closeAll(triples);
	}
}

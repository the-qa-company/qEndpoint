package com.the_qa_company.qendpoint.core.dictionary.impl.kcat;

import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.util.concurrent.SyncSeq;
import com.the_qa_company.qendpoint.core.util.crc.CRC;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Mapping storage for KCat, backed up to disk after dictionary merge for future
 * usage, close its location Path after usage to handle CloseSuppressPath.
 *
 * @author Antoine Willerval
 */
public class KCatMapping implements Closeable {
	// file magic
	private static final byte[] HEADER_MAGIC = "$KCM".getBytes(StandardCharsets.UTF_8);
	private static final byte LANG_FLAG = 0x1;
	private static final byte TYPED_FLAG = 0x2;

	private final Path location;
	final SyncSeq[] subjectsMaps;
	final SyncSeq[] predicatesMaps;
	final SyncSeq[] objectsMaps;
	private final boolean langHDT;
	private final boolean typedHDT;
	private final int shift;
	private final int datasetCount;
	private final long[] countSubject;
	private final long[] countPredicate;
	private final long[] countObject;
	private final int numbitsS;
	private final int numbitsP;
	private final int numbitsO;

	private long countShared;
	private long countNonTyped;
	private long countTyped;

	public KCatMapping(Path location, int datasetCount, long[] countSubject, long[] countPredicate, long[] countObject,
			int numbitsS, int numbitsP, int numbitsO, boolean langHDT, boolean typedHDT) throws IOException {
		this.location = location;
		this.datasetCount = datasetCount;
		if (datasetCount < 0) {
			throw new IllegalArgumentException("Negative number of datasets!");
		}
		subjectsMaps = new SyncSeq[datasetCount];
		predicatesMaps = new SyncSeq[datasetCount];
		objectsMaps = new SyncSeq[datasetCount];

		this.countSubject = countSubject;
		this.countPredicate = countPredicate;
		this.countObject = countObject;
		this.numbitsS = numbitsS;
		this.numbitsP = numbitsP;
		this.numbitsO = numbitsO;
		this.langHDT = langHDT;
		this.typedHDT = typedHDT;

		if (typedHDT) {
			shift = 2;
		} else {
			shift = 1;
		}

		Files.createDirectories(location);

		for (int i = 0; i < datasetCount; i++) {
			subjectsMaps[i] = new SyncSeq(new SequenceLog64BigDisk(
					location.resolve("subjectsMap_" + i).toAbsolutePath().toString(), numbitsS, countSubject[i] + 1));
			predicatesMaps[i] = new SyncSeq(
					new SequenceLog64BigDisk(location.resolve("predicatesMap_" + i).toAbsolutePath().toString(),
							numbitsP, countPredicate[i] + 1));
			objectsMaps[i] = new SyncSeq(new SequenceLog64BigDisk(
					location.resolve("objectsMap_" + i).toAbsolutePath().toString(), numbitsO, countObject[i] + 1));
		}

	}

	public KCatMapping(Path location) throws IOException {
		this.location = location;
		try (FileChannel ch = FileChannel.open(location.resolve("header.kcm"), StandardOpenOption.READ)) {
			try (CloseMappedByteBuffer buff = IOUtil.mapChannel(location.resolve("header.kcm"), ch,
					FileChannel.MapMode.READ_ONLY, 0, HEADER_MAGIC.length + Integer.BYTES)) {
				byte[] magic = new byte[HEADER_MAGIC.length];
				buff.get(0, magic);
				for (int i = 0; i < magic.length; i++) {
					if (magic[i] != HEADER_MAGIC[i]) {
						throw new IOException("Bad KCM header");
					}
				}

				this.datasetCount = buff.getInt(magic.length);
				if (datasetCount < 0) {
					throw new IOException("Bad KCM dataset count");
				}
			}
			subjectsMaps = new SyncSeq[datasetCount];
			predicatesMaps = new SyncSeq[datasetCount];
			objectsMaps = new SyncSeq[datasetCount];

			CRC crc = new CRC32();
			long headerSize = HEADER_MAGIC.length + Long.BYTES * datasetCount * 3L + Integer.BYTES * 4L + crc.sizeof()
					+ 1 + Long.BYTES * 3;
			try (CloseMappedByteBuffer buff = IOUtil.mapChannel(location.resolve("header.kcm"), ch,
					FileChannel.MapMode.READ_ONLY, 0, headerSize)) {
				// check CRC value
				crc.update(buff, 0, (int) headerSize - crc.sizeof());
				if (!crc.readAndCheck(buff, (int) headerSize - crc.sizeof())) {
					throw new CRCException("CRC Error while reading QEPMap header.");
				}
				int size = HEADER_MAGIC.length + Integer.BYTES; // magic + ds
																// count

				numbitsS = buff.getInt(size);
				size += Integer.BYTES;
				numbitsP = buff.getInt(size);
				size += Integer.BYTES;
				numbitsO = buff.getInt(size);
				size += Integer.BYTES;

				countShared = buff.getLong(size);
				size += Long.BYTES;
				countNonTyped = buff.getLong(size);
				size += Long.BYTES;
				countTyped = buff.getLong(size);
				size += Long.BYTES;

				countSubject = new long[datasetCount];
				countPredicate = new long[datasetCount];
				countObject = new long[datasetCount];

				for (int i = 0; i < datasetCount; i++) {
					countSubject[i] = buff.getLong(size);
					size += Long.BYTES;
					countPredicate[i] = buff.getLong(size);
					size += Long.BYTES;
					countObject[i] = buff.getLong(size);
					size += Long.BYTES;

					subjectsMaps[i] = new SyncSeq(
							new SequenceLog64BigDisk(location.resolve("subjectsMap_" + i).toAbsolutePath().toString(),
									numbitsS, countSubject[i] + 1));
					predicatesMaps[i] = new SyncSeq(
							new SequenceLog64BigDisk(location.resolve("predicatesMap_" + i).toAbsolutePath().toString(),
									numbitsP, countPredicate[i] + 1));
					objectsMaps[i] = new SyncSeq(
							new SequenceLog64BigDisk(location.resolve("objectsMap_" + i).toAbsolutePath().toString(),
									numbitsO, countObject[i] + 1));
				}

				byte flags = buff.get(size);
				size += 1;
				typedHDT = (flags & TYPED_FLAG) != 0;
				langHDT = (flags & LANG_FLAG) != 0;
				assert headerSize - crc.sizeof() == size : "bad size reading";
			}
		}

		if (typedHDT) {
			shift = 2;
		} else {
			shift = 1;
		}
	}

	public void writeHeader() throws IOException {
		CRC crc = new CRC32();
		long headerSize = HEADER_MAGIC.length + Long.BYTES * datasetCount * 3L + Integer.BYTES * 4L + crc.sizeof() + 1
				+ Long.BYTES * 3;

		Files.createDirectories(location);
		try (FileChannel ch = FileChannel.open(location.resolve("header.kcm"), StandardOpenOption.WRITE,
				StandardOpenOption.READ, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
				CloseMappedByteBuffer buff = IOUtil.mapChannel(location.resolve("header.kcm"), ch,
						FileChannel.MapMode.READ_WRITE, 0, headerSize)) {
			int size = 0;
			buff.put(size, HEADER_MAGIC);
			size += HEADER_MAGIC.length;

			// dataset count
			buff.putInt(size, datasetCount);
			size += Integer.BYTES;

			// bits
			buff.putInt(size, numbitsS);
			size += Integer.BYTES;
			buff.putInt(size, numbitsP);
			size += Integer.BYTES;
			buff.putInt(size, numbitsO);
			size += Integer.BYTES;

			buff.putLong(size, countShared);
			size += Long.BYTES;
			buff.putLong(size, countNonTyped);
			size += Long.BYTES;
			buff.putLong(size, countTyped);
			size += Long.BYTES;

			for (int i = 0; i < datasetCount; i++) {
				buff.putLong(size, countSubject[i]);
				size += Long.BYTES;
				buff.putLong(size, countPredicate[i]);
				size += Long.BYTES;
				buff.putLong(size, countObject[i]);
				size += Long.BYTES;
			}

			byte flags = 0;

			if (typedHDT) {
				flags |= TYPED_FLAG;
			}
			if (langHDT) {
				flags |= LANG_FLAG;
			}

			buff.put(size, flags);
			size += 1;

			crc.update(buff, 0, size);
			crc.writeCRC(buff, size);
			size += crc.sizeof();
			assert headerSize == size : "bad header allocation";
		}
	}

	/**
	 * test if a header value is shared
	 *
	 * @param headerValue header value
	 * @return true if the header is shared, false otherwise
	 */
	public boolean isShared(long headerValue) {
		return (headerValue & KCatMerger.SHARED_MASK) != 0;
	}

	/**
	 * test if a header value is typed
	 *
	 * @param headerValue header value
	 * @return true if the header is typed, false otherwise
	 */
	public boolean isTyped(long headerValue) {
		return typedHDT && (headerValue & KCatMerger.TYPED_MASK) != 0;
	}

	/**
	 * extract the subject from an HDT
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractSubject(int hdtIndex, long oldID) {
		long headerID = subjectsMaps[hdtIndex].get(oldID);
		if (isShared(headerID)) {
			return headerID >>> shift;
		}
		return (headerID >>> shift) + countShared;
	}

	/**
	 * extract the predicate from an HDT
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractPredicate(int hdtIndex, long oldID) {
		return predicatesMaps[hdtIndex].get(oldID);
	}

	/**
	 * extract the object from an HDT
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractObject(int hdtIndex, long oldID) {
		long headerID = objectsMaps[hdtIndex].get(oldID);
		if (isShared(headerID)) {
			return headerID >>> shift;
		}
		if (isTyped(headerID)) {
			if (langHDT) {
				// in a MSDL the NDT section is before the DT/LG sections
				return (headerID >>> shift) + countShared + countNonTyped;
			}
			return (headerID >>> shift) + countShared;
		}
		if (langHDT) {
			return (headerID >>> shift) + countShared;
		}
		return (headerID >>> shift) + countShared + countTyped;
	}

	public void setCountNonTyped(long countNonTyped) {
		this.countNonTyped = countNonTyped;
	}

	public void setCountShared(long countShared) {
		this.countShared = countShared;
	}

	public void setCountTyped(long countTyped) {
		this.countTyped = countTyped;
	}

	/**
	 * get the subject map for a dataset
	 *
	 * @param dataset the dataset
	 * @return map
	 */
	public SyncSeq getSubjectsMap(int dataset) {
		return subjectsMaps[dataset];
	}

	/**
	 * get the predicate map for a dataset
	 *
	 * @param dataset the dataset
	 * @return map
	 */
	public SyncSeq getPredicatesMap(int dataset) {
		return predicatesMaps[dataset];
	}

	/**
	 * get the object map for a dataset
	 *
	 * @param dataset the dataset
	 * @return map
	 */
	public SyncSeq getObjectsMap(int dataset) {
		return objectsMaps[dataset];
	}

	@Override
	public void close() throws IOException {
		// close location, if it's a CloseSuppressPath it'll delete it
		Closer.closeAll(subjectsMaps, predicatesMaps, objectsMaps, location);
	}
}

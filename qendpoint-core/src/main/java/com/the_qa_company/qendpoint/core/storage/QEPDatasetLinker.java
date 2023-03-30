package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.header.Header;
import com.the_qa_company.qendpoint.core.util.io.CloseMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class QEPDatasetLinker implements Closeable {
	private static final long HEADER_SIZE;
	private static final byte[] COOKIE = "$QML".getBytes(ByteStringUtil.STRING_ENCODING);

	static {
		long headerSize = 0;

		// cookie: byte[COOKIE.length]
		headerSize += COOKIE.length * Byte.BYTES;
		// config: long
		headerSize += Long.BYTES;
		// id sizes: (ID size + 1) * 2 (+1 for '\0')
		headerSize += (QEPCore.MAX_ID_SIZE + 1) * Byte.BYTES * 2;
		// 4 sections * 2 datasets
		headerSize += Long.BYTES * 4 * 2;

		HEADER_SIZE = headerSize;
	}

	/**
	 * UID class for this linker
	 *
	 * @param uid1 uid of the lower dataset
	 * @param uid2 uid of the greater dataset
	 */
	public record Uid(long uid1, long uid2) {
	}

	private final QEPDataset dataset1;
	private final QEPDataset dataset2;

	private final Path mapPath;
	private final Uid uid;
	private FileChannel channel;
	private long config;

	public QEPDatasetLinker(Path parent, QEPDataset dataset1, QEPDataset dataset2) {
		String id1 = dataset1.id();
		String id2 = dataset2.id();

		int c = id1.compareTo(id2);

		if (c == 0) {
			throw new IllegalFormatException("Can't link a dataset to itself!");
		}

		// we use the lower id as our dataset 1 and greater id as our dataset 2

		if (c < 0) {
			this.dataset1 = dataset1;
			this.dataset2 = dataset2;
		} else { // we switch the dataset order
			this.dataset1 = dataset2;
			this.dataset2 = dataset1;
		}

		mapPath = parent.resolve("map-" + getMapId() + ".qepm");
		uid = new Uid(this.dataset1.uid(), this.dataset2.uid());
	}

	/**
	 * Sync the linker object
	 *
	 * @throws IOException error while syncing
	 */
	public void sync() throws IOException {
		boolean created = !Files.exists(mapPath);
		// a small race condition, but let's ignore it
		try {
			channel = FileChannel.open(mapPath, StandardOpenOption.READ, StandardOpenOption.WRITE,
					StandardOpenOption.CREATE);

			try (CloseMappedByteBuffer header = IOUtil.mapChannel(mapPath, channel, FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE)) {
				int shift = 0;
				if (created) {
					// we write the header
					// write cookie
					for (; shift < COOKIE.length; shift++) {
						header.put(shift, COOKIE[shift]);
					}
					int configShift = shift++;

					IOUtil.writeCString(header, dataset1.id(), shift);
					shift += QEPCore.MAX_ID_SIZE + 1;
					IOUtil.writeCString(header, dataset2.id(), shift);
					shift += QEPCore.MAX_ID_SIZE + 1;
					Dictionary d1 = dataset1.dataset().getDictionary();
					long nshared1 = d1.getNshared();
					header.putLong(shift, nshared1);
					header.putLong(shift += Long.BYTES, d1.getNsubjects() - nshared1);
					header.putLong(shift += Long.BYTES, d1.getNpredicates());
					header.putLong(shift += Long.BYTES, d1.getNobjects() - nshared1);
					shift += Long.BYTES;

					Dictionary d2 = dataset1.dataset().getDictionary();
					long nshared2 = d2.getNshared();
					header.putLong(shift, nshared2);
					header.putLong(shift += Long.BYTES, d2.getNsubjects() - nshared2);
					header.putLong(shift += Long.BYTES, d2.getNpredicates());
					header.putLong(shift += Long.BYTES, d2.getNobjects() - nshared2);
					shift += Long.BYTES;



					header.putLong(configShift, config);
				} else {
					// we check the cookie
					for (; shift < COOKIE.length; shift++) {
						byte cc = header.get(shift);
						if (cc != COOKIE[shift]) {
							throw new IOException("Can't read cookie of dataset linker " + getMapId() + "!");
						}
					}
					config = header.getLong(shift++);

					String aid1 = IOUtil.readCString(header, shift, QEPCore.MAX_ID_SIZE + 1);
					shift += QEPCore.MAX_ID_SIZE + 1;
					if (!aid1.equals(dataset1.id())) {
						throw new IOException("read dataset id1=" + aid1 + " but use " + dataset1.id());
					}

					String aid2 = IOUtil.readCString(header, shift, QEPCore.MAX_ID_SIZE + 1);
					shift += QEPCore.MAX_ID_SIZE + 1;
					if (!aid2.equals(dataset2.id())) {
						throw new IOException("read dataset id2=" + aid2 + " but use " + dataset2.id());
					}

					checkHeader(header, shift,"1", this.dataset1);
					shift += Long.BYTES * 4;
					checkHeader(header, shift,"2", this.dataset2);
					shift += Long.BYTES * 4;

				}
			}
		} catch (Throwable t) {
			try {
				close();
			} catch (Exception t2) {
				t.addSuppressed(t2);
			} catch (Error e) {
				e.addSuppressed(t);
				throw e;
			}
			throw t;
		}
	}

	private void checkHeader(CloseMappedByteBuffer header, int shift, String id, QEPDataset dataset) throws IOException {
		Dictionary d = dataset.dataset().getDictionary();
		long nshared = d.getNshared();
		long nsubject = d.getNsubjects() - nshared;
		long npred = d.getNpredicates();
		long nobject = d.getNobjects() - nshared;

		long sh = header.getLong(shift);
		if (sh != nshared) {
			throw new IOException("Bad count for section SHARED for dataset header " + id + " " + sh + "!=" + nshared);
		}

		long s = header.getLong(shift + Long.BYTES);
		if (s != nsubject) {
			throw new IOException("Bad count for section SUBJECT for dataset header " + id + " " + s + "!=" + nsubject);
		}

		long p = header.getLong(shift + Long.BYTES * 2);
		if (p != npred) {
			throw new IOException("Bad count for section PREDICATE for dataset header " + id + " " + p + "!=" + npred);
		}

		long o = header.getLong(shift + Long.BYTES * 3);
		if (o != nobject) {
			throw new IOException("Bad count for section OBJECT for dataset header " + id + " " + o + "!=" + nobject);
		}
	}

	/**
	 * @return Map id
	 */
	public String getMapId() {
		return this.dataset1.id() + "-" + this.dataset2.id();
	}

	/**
	 * @return UID only valid as long as the same QEPCore is used
	 */
	public Uid getUid() {
		return uid;
	}

	/**
	 * @return the map path of this linker object
	 */
	public Path getMapPath() {
		return mapPath;
	}

	@Override
	public void close() throws IOException {
		try {
			if (channel != null) {
				channel.close();
			}
		} finally {
			channel = null;
		}
	}
}

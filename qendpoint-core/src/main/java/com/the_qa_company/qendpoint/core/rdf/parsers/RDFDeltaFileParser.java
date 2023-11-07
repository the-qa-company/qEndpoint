package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.WikidataChangesFlavor;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherExceptionIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class RDFDeltaFileParser implements RDFParserCallback {
	public static final byte[] COOKIE = "$DltF0\n\r".getBytes(US_ASCII);

	public record DeltaFileComponent(String fileName, byte[] data) {}

	public static class DeltaFileReader extends FetcherExceptionIterator<DeltaFileComponent, IOException>
			implements Closeable {
		private final long count;
		private long id;
		private final InputStream stream;
		private final long start;
		private final long end;
		private final WikidataChangesFlavor flavor;
		private boolean noExceptionOnlyStop;

		public DeltaFileReader(InputStream is, HDTOptions spec) throws IOException {
			boolean nocrc = spec.getBoolean(HDTOptionsKeys.PARSER_DELTAFILE_NO_CRC, false);
			noExceptionOnlyStop = spec.getBoolean(HDTOptionsKeys.PARSER_DELTAFILE_NO_EXCEPTION, false);

			this.stream = nocrc ? is : new CRCInputStream(is, new CRC8());

			if (!Arrays.equals(stream.readNBytes(8), COOKIE)) {
				throw new IOException("Bad cookie");
			}

			this.count = IOUtil.readLong(stream);
			this.start = IOUtil.readLong(stream);
			this.end = IOUtil.readLong(stream);
			this.flavor = WikidataChangesFlavor.getFromId((byte) stream.read());
			if (flavor == null) {
				throw new IOException("Bad flavor");
			}
			stream.skipNBytes(3);

			if (!nocrc) {
				CRCInputStream crcis = (CRCInputStream) stream;
				if (!crcis.readCRCAndCheck()) {
					throw new IOException("Bad header crc");
				}
				crcis.setCRC(new CRC32());
			} else {
				stream.skipNBytes(1); // skip header crc
			}
		}

		public Instant getStart() {
			return Instant.ofEpochSecond(start / 1000000, (start % 1000000) * 1000);
		}

		public Instant getEnd() {
			return Instant.ofEpochSecond(end / 1000000, (end % 1000000) * 1000);
		}

		@Override
		public long getSize() {
			return count;
		}

		public void setNoExceptionOnlyStop(boolean noExceptionOnlyStop) {
			this.noExceptionOnlyStop = noExceptionOnlyStop;
		}

		public WikidataChangesFlavor getFlavor() {
			return flavor;
		}

		@Override
		protected DeltaFileComponent getNext() throws IOException {
			if (id >= count) {
				if (id == count) { // last
					id++;
					try {
						if (stream instanceof CRCInputStream crcis) {
							if (!crcis.readCRCAndCheck()) {
								throw new IOException("Bad data crc!");
							}
						} else {
							stream.readNBytes(4); // read crc
						}
					} catch (Throwable t) {
						if (noExceptionOnlyStop) {
							return null;
						}
						throw t;
					}
				}
				return null;
			}
			id++;

			try {
				// name
				byte[] name = IOUtil.readSizedBuffer(stream, ProgressListener.ignore()); // title
																							// +
																							// .ttl?
				// buffer
				byte[] bytes = IOUtil.readSizedBuffer(stream, ProgressListener.ignore());

				return new DeltaFileComponent(new String(name, US_ASCII), bytes);
			} catch (Throwable e) {
				if (noExceptionOnlyStop) {
					return null;
				}
				throw e;
			}
		}

		@Override
		public void close() throws IOException {
			stream.close();
		}
	}

	private final HDTOptions spec;

	public RDFDeltaFileParser(HDTOptions spec) {
		this.spec = spec;
	}

	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		try (InputStream is = IOUtil.getFileInputStream(fileName)) {
			doParse(is, baseUri, notation, keepBNode, callback);
		} catch (IOException e) {
			throw new ParserException(e);
		}
	}

	@Override
	public void doParse(InputStream in, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback)
			throws ParserException {
		try {
			// read df file
			try (DeltaFileReader reader = new DeltaFileReader(in, spec)) {
				while (reader.hasNext()) {
					DeltaFileComponent next = reader.next();
					if (next.data.length == 0) {
						continue; // deleted
					}
					RDFNotation not = RDFNotation.guess(next.fileName);
					RDFParserCallback parser = RDFParserFactory.getParserCallback(not, spec);
					try {
						// read the next byte information
						parser.doParse(new GZIPInputStream(new ByteArrayInputStream(next.data)), baseUri, not,
								keepBNode, callback);
					} catch (IOException e) {
						throw new ParserException("Error when reading " + next.fileName + " size: " + next.data.length,
								e);
					}
				}
			}
		} catch (IOException e) {
			throw new ParserException(e);
		}
	}

}

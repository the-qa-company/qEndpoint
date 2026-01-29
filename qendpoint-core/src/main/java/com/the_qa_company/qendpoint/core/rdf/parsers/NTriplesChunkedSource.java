package com.the_qa_company.qendpoint.core.rdf.parsers;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.iterator.utils.SizedSupplier;
import com.the_qa_company.qendpoint.core.quad.QuadString;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.UnicodeEscape;
import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionSupplier;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.string.ByteStringInterner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chunk factory for N-Triples / N-Quads. Pull-based end-to-end: a KWay worker
 * asks for a chunk supplier (this.get()). Stream-backed instances read lines
 * under a small lock, while {@link Path}-backed instances memory-map the file
 * and hand out zero-copy byte slices. Both paths yield
 * {@link TripleString}/{@link QuadString} via {@link SizedSupplier#get()}.
 */
public final class NTriplesChunkedSource
		implements ExceptionSupplier<SizedSupplier<TripleString>, IOException>, Closeable {

	private static final Logger log = LoggerFactory.getLogger(NTriplesChunkedSource.class);
	private static final byte LF = (byte) '\n';
	private static final byte CR = (byte) '\r';
	private static final byte BS = (byte) '\\';
	private static final int DEFAULT_INTERN_ENTRIES = 4096;
	private static final int MAX_INTERN_ENTRIES = 262_144;
	private static final ThreadLocal<ByteStringInterner> SHARED_INTERNER = ThreadLocal
			.withInitial(() -> new ByteStringInterner(DEFAULT_INTERN_ENTRIES, MAX_INTERN_ENTRIES));

	private final boolean mmapMode;

	private final BufferedReader reader;
	private final LineReader lineReader;
	private final Object lock = new Object();

	private final BigMappedByteBuffer mapped;
	private final FileChannel channel;
	private final long fileSize;
	private final AtomicLong nextOffset;
	private final long probeStep;
	private final int expectedInternerEntries;

	private final boolean readQuad;
	private final long chunkBudgetBytes;
	private final BlankNodeIdMapper bnodeMapper;

	// limits how much we buffer at once (prevents "double buffering" the whole
	// chunk in RAM)
	private final long maxBatchBytes;
	private final int maxBatchLines;

	private volatile boolean eof;

	public NTriplesChunkedSource(InputStream input, RDFNotation notation, long chunkBudgetBytes) {
		this(input, notation, chunkBudgetBytes, 8L * 1024 * 1024, 8192, null);
	}

	public NTriplesChunkedSource(InputStream input, RDFNotation notation, long chunkBudgetBytes, long maxBatchBytes,
			int maxBatchLines) {
		this(input, notation, chunkBudgetBytes, maxBatchBytes, maxBatchLines, null);
	}

	public NTriplesChunkedSource(InputStream input, RDFNotation notation, long chunkBudgetBytes, long maxBatchBytes,
			int maxBatchLines, BlankNodeIdMapper bnodeMapper) {
		Objects.requireNonNull(input, "input");
		Objects.requireNonNull(notation, "notation");

		if (notation != RDFNotation.NTRIPLES && notation != RDFNotation.NQUAD) {
			throw new IllegalArgumentException("NTriplesChunkedSource supports only NTRIPLES/NQUAD, got " + notation);
		}
		if (chunkBudgetBytes <= 0) {
			throw new IllegalArgumentException("chunkBudgetBytes must be > 0");
		}
		if (maxBatchBytes <= 0) {
			throw new IllegalArgumentException("maxBatchBytes must be > 0");
		}
		if (maxBatchLines <= 0) {
			throw new IllegalArgumentException("maxBatchLines must be > 0");
		}

		this.reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
		this.lineReader = new LineReader();
		this.readQuad = notation == RDFNotation.NQUAD;
		this.chunkBudgetBytes = chunkBudgetBytes;
		this.maxBatchBytes = maxBatchBytes;
		this.maxBatchLines = maxBatchLines;
		this.bnodeMapper = bnodeMapper;
		this.expectedInternerEntries = Math.max(DEFAULT_INTERN_ENTRIES,
				estimateInternerEntries(maxBatchLines, readQuad));
		this.mmapMode = false;
		this.mapped = null;
		this.channel = null;
		this.fileSize = 0L;
		this.nextOffset = null;
		this.probeStep = 0L;
	}

	public NTriplesChunkedSource(Path path, RDFNotation notation, long chunkBudgetBytes) throws IOException {
		this(path, notation, chunkBudgetBytes, null);
	}

	public NTriplesChunkedSource(Path path, RDFNotation notation, long chunkBudgetBytes, BlankNodeIdMapper bnodeMapper)
			throws IOException {
		Objects.requireNonNull(path, "path");
		Objects.requireNonNull(notation, "notation");

		if (notation != RDFNotation.NTRIPLES && notation != RDFNotation.NQUAD) {
			throw new IllegalArgumentException("NTriplesChunkedSource supports only NTRIPLES/NQUAD, got " + notation);
		}
		if (chunkBudgetBytes <= 0) {
			throw new IllegalArgumentException("chunkBudgetBytes must be > 0");
		}

		this.reader = null;
		this.lineReader = null;
		this.mmapMode = true;
		this.readQuad = notation == RDFNotation.NQUAD;
		this.chunkBudgetBytes = chunkBudgetBytes;
		this.maxBatchBytes = 0L;
		this.maxBatchLines = 0;
		this.bnodeMapper = bnodeMapper;
		this.nextOffset = new AtomicLong(0L);
		this.probeStep = Math.max(1L, chunkBudgetBytes / 8L);
		this.expectedInternerEntries = Math.max(DEFAULT_INTERN_ENTRIES,
				estimateInternerEntries(estimateLineCount(chunkBudgetBytes), readQuad));

		FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
		BigMappedByteBuffer mappedBuffer;
		long size;
		try {
			size = ch.size();
			mappedBuffer = BigMappedByteBuffer.ofFileChannel(path.toString(), ch, FileChannel.MapMode.READ_ONLY, 0L,
					size);
		} catch (IOException e) {
			try {
				ch.close();
			} finally {
				throw e;
			}
		}

		this.channel = ch;
		this.mapped = mappedBuffer;
		this.fileSize = size;
	}

	/**
	 * @return next chunk supplier, or null when input is exhausted.
	 */
	@Override
	public SizedSupplier<TripleString> get() throws IOException {
		if (mmapMode) {
			return nextMappedChunk();
		}
		Chunk chunk = new Chunk();
		if (!chunk.prime()) {
			return null;
		}
		return chunk;
	}

	private SizedSupplier<TripleString> nextMappedChunk() {
		while (true) {
			long start = nextOffset.get();
			if (start >= fileSize) {
				return null;
			}
			long probe = Math.min(start + probeStep, fileSize);
			long end = findNextUnescapedLineBreakEnd(mapped, probe, fileSize);
			if (end <= start) {
				end = fileSize;
			}
			if (nextOffset.compareAndSet(start, end)) {
				return new MmapChunk(start, end);
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (mmapMode) {
			try {
				mapped.close();
			} finally {
				channel.close();
			}
			return;
		}
		synchronized (lock) {
			eof = true;
		}
		reader.close();
	}

	private final class Chunk implements SizedSupplier<TripleString> {
		private final LineSliceBuffer lineBuffer;
		private ByteStringInterner interner;
		private TripleString.ComponentDecoderChars decoder;
		private int idx;

		private long remaining = chunkBudgetBytes;
		private long size;
		private boolean done;

		private final TripleString reusable = readQuad ? new QuadString() : new TripleString();

		private Chunk() {
			int initialCapacity = (int) Math.min(Integer.MAX_VALUE - 8L, Math.max(64L, maxBatchBytes));
			this.lineBuffer = new LineSliceBuffer(maxBatchLines, initialCapacity);
		}

		private boolean prime() throws IOException {
			fillBuffer();
			return !lineBuffer.isEmpty();
		}

		@Override
		public TripleString get() {
			if (done) {
				return null;
			}

			try {
				while (true) {
					ensureDecoder();
					// Need more lines?
					if (idx >= lineBuffer.count()) {
						idx = 0;

						// stop condition for this chunk
						if (remaining <= 0) {
							done = true;
							return null;
						}

						fillBuffer();
						if (lineBuffer.isEmpty()) {
							done = true;
							return null;
						}
					}

					char[] slab = lineBuffer.slab();
					int start = lineBuffer.startAt(idx);
					int endExclusive = lineBuffer.endAt(idx);
					idx++;
					if (parseLine(slab, start, endExclusive - 1, reusable, readQuad, decoder)) {
						remapBNodes(reusable);
						return reusable;
					}
					// skip comments/blank/invalid lines, keep scanning
				}
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}

		private void ensureDecoder() {
			if (decoder == null) {
				interner = SHARED_INTERNER.get();
				interner.ensureCapacity(expectedInternerEntries);
				decoder = (buffer, start, end) -> UnicodeEscape.unescapeByteString(buffer, start, end, interner);
			}
		}

		@Override
		public long getSize() {
			return size;
		}

		private void fillBuffer() throws IOException {
			// cap in-memory buffering
			long batchBudget = Math.min(remaining, maxBatchBytes);

			lineBuffer.reset();
			synchronized (lock) {
				while (!eof && lineBuffer.count() < maxBatchLines && batchBudget > 0) {
					int lineLen = lineReader.readLine(reader, lineBuffer);
					if (lineLen < 0) {
						eof = true;
						break;
					}
					if (lineLen == 0) {
						continue;
					}

					long est = (long) lineLen + 1L;
					size += est;
					remaining -= est;
					batchBudget -= est;
				}
			}
		}
	}

	private final class MmapChunk implements SizedSupplier<TripleString> {
		private final long end;
		private final long size;
		private ByteStringInterner interner;
		private long cursor;
		private boolean done;

		private final TripleString reusable = readQuad ? new QuadString() : new TripleString();

		private MmapChunk(long start, long end) {
			this.cursor = start;
			this.end = end;
			this.size = end - start;
		}

		@Override
		public TripleString get() {
			if (done) {
				return null;
			}

			ensureInterner();
			while (cursor < end) {
				long lineStart = cursor;
				long lineBreak = findNextLineBreak(mapped, lineStart, end);
				long lineEndExclusive;
				if (lineBreak < end) {
					lineEndExclusive = lineBreak;
					if (lineEndExclusive > lineStart && byteAt(mapped, lineEndExclusive - 1) == CR) {
						lineEndExclusive--;
					}
					cursor = lineBreak + 1;
				} else {
					lineEndExclusive = end;
					if (lineEndExclusive > lineStart && byteAt(mapped, lineEndExclusive - 1) == CR) {
						lineEndExclusive--;
					}
					cursor = end;
				}

				if (lineEndExclusive <= lineStart) {
					continue;
				}

				if (parseLine(mapped, lineStart, lineEndExclusive - 1, reusable, readQuad, interner)) {
					remapBNodes(reusable);
					return reusable;
				}
				// skip comments/blank/invalid lines, keep scanning
			}

			done = true;
			return null;
		}

		private void ensureInterner() {
			if (interner == null) {
				interner = SHARED_INTERNER.get();
				interner.ensureCapacity(expectedInternerEntries);
			}
		}

		@Override
		public long getSize() {
			return size;
		}
	}

	private static int estimateLineCount(long byteCount) {
		if (byteCount <= 0) {
			return 256;
		}
		long lines = byteCount / 64L;
		long clamped = Math.max(256L, Math.min(65536L, lines));
		return (int) clamped;
	}

	private static int estimateInternerEntries(long lineCount, boolean readQuad) {
		long entries = lineCount * (readQuad ? 4L : 3L);
		long clamped = Math.max(256L, Math.min(262144L, entries));
		return (int) clamped;
	}

	private void remapBNodes(TripleString triple) {
		if (bnodeMapper == null) {
			return;
		}
		bnodeMapper.remap(triple, readQuad);
	}

	/**
	 * Mirrors {@link RDFParserSimple}'s whitespace trimming and comment
	 * skipping.
	 */
	private static boolean parseLine(char[] buffer, int start, int endInclusive, TripleString out, boolean readQuad,
			TripleString.ComponentDecoderChars decoder) {
		int s = start;
		while (s <= endInclusive) {
			char c = buffer[s];
			if (c != ' ' && c != '\t') {
				break;
			}
			s++;
		}

		int e = endInclusive;
		while (e >= s) {
			char c = buffer[e];
			if (c != ' ' && c != '\t') {
				break;
			}
			e--;
		}

		if (s + 1 >= e) {
			return false;
		}
		if (buffer[s] == '#') {
			return false;
		}

		try {
			out.readByteString(buffer, s, e, readQuad, decoder);
			if (!out.hasEmpty()) {
				return true;
			}

			if (log.isWarnEnabled()) {
				log.warn("Could not parse triple, ignored.\n{}", segmentToString(buffer, s, e));
			}
			return false;
		} catch (Exception e1) {
			if (log.isWarnEnabled()) {
				log.warn("Could not parse triple, ignored.\n{}", segmentToString(buffer, s, e));
			}
			return false;
		}
	}

	private static boolean parseLine(BigMappedByteBuffer buffer, long start, long end, TripleString out,
			boolean readQuad, ByteStringInterner interner) {
		long s = start;
		while (s <= end) {
			byte b = byteAt(buffer, s);
			if (b != ' ' && b != '\t') {
				break;
			}
			s++;
		}

		long e = end;
		while (e >= s) {
			byte b = byteAt(buffer, e);
			if (b != ' ' && b != '\t') {
				break;
			}
			e--;
		}

		if (s + 1 >= e) {
			return false;
		}
		if (byteAt(buffer, s) == '#') {
			return false;
		}

		try {
			readByteString(buffer, s, e, readQuad, out, interner);
			if (!out.hasEmpty()) {
				return true;
			}

			if (log.isWarnEnabled()) {
				log.warn("Could not parse triple, ignored.\n{}", segmentToString(buffer, s, e));
			}
			return false;
		} catch (Exception e1) {
			if (log.isWarnEnabled()) {
				log.warn("Could not parse triple, ignored.\n{}", segmentToString(buffer, s, e));
			}
			return false;
		}
	}

	private static void readByteString(BigMappedByteBuffer buffer, long start, long endInclusive, boolean processQuad,
			TripleString out, ByteStringInterner interner) throws ParserException {
		long end = endInclusive + 1;
		out.clear();

		long split = searchNextTabOrSpace(buffer, start, end);
		if (split == -1) {
			return;
		}

		long posa = start;
		long posb = split;

		if (byteAt(buffer, posa) == '<') {
			posa++;
			if (posb > posa && byteAt(buffer, posb - 1) == '>') {
				posb--;
			}
		}
		out.setSubject(UnicodeEscape.unescapeByteString(buffer, posa, posb, interner));

		posa = split + 1;
		split = searchNextTabOrSpace(buffer, posa, end);
		if (split == -1) {
			return;
		}
		posb = split;

		if (byteAt(buffer, posa) == '<') {
			posa++;
			if (posb > posa && byteAt(buffer, posb - 1) == '>') {
				posb--;
			}
		}
		out.setPredicate(UnicodeEscape.unescapeByteString(buffer, posa, posb, interner));

		posa = split + 1;
		posb = end;

		if (posb <= posa) {
			return;
		}
		if (byteAt(buffer, posb - 1) == '.') {
			posb--;
		}
		if (posb <= posa) {
			return;
		}
		byte prev = byteAt(buffer, posb - 1);
		if (prev == ' ' || prev == '\t') {
			posb--;
		}
		if (posb <= posa) {
			return;
		}

		if (processQuad) {
			byte lastElem = byteAt(buffer, posb - 1);
			if (lastElem != '"') {
				if (lastElem == '>') {
					long iriStart = lastIndexOf(buffer, (byte) '<', posb - 1, posa);
					if (iriStart < posa) {
						throw new ParserException("end of a '>' without a start '<'");
					}
					if (posa != iriStart && iriStart > 0 && byteAt(buffer, iriStart - 1) != '^') {
						out.setGraph(UnicodeEscape.unescapeByteString(buffer, iriStart + 1, posb - 1, interner));
						posb = iriStart - 1;
					}
				} else {
					long bnodeStart = searchBNodeBackward(buffer, posa, posb);
					if (bnodeStart > posa) {
						out.setGraph(UnicodeEscape.unescapeByteString(buffer, bnodeStart + 1, posb, interner));
						posb = bnodeStart;
					}
				}
			}
		}

		if (byteAt(buffer, posa) == '<') {
			posa++;
			if (posb > posa && byteAt(buffer, posb - 1) == '>') {
				posb--;
			}
		}
		out.setObject(UnicodeEscape.unescapeByteString(buffer, posa, posb, interner));
	}

	private static long searchNextTabOrSpace(BigMappedByteBuffer buffer, long start, long end) {
		for (long i = start; i < end; i++) {
			byte b = byteAt(buffer, i);
			if (b == ' ' || b == '\t') {
				return i;
			}
		}
		return -1L;
	}

	private static long searchBNodeBackward(BigMappedByteBuffer buffer, long start, long end) {
		long loc = end - 1;

		while (loc >= start) {
			byte b = byteAt(buffer, loc);
			switch (b) {
			case ' ', '\t' -> {
				if (loc + 2 >= end) {
					return -1;
				}
				if (byteAt(buffer, loc + 1) == '_' && byteAt(buffer, loc + 2) == ':') {
					return loc;
				}
			}
			case '^', '@', '>', '<', '"' -> {
				return -1;
			}
			default -> {
			}
			}
			loc--;
		}
		return -1;
	}

	private static long lastIndexOf(BigMappedByteBuffer buffer, byte value, long from, long min) {
		long i = from;
		while (i >= min) {
			if (byteAt(buffer, i) == value) {
				return i;
			}
			i--;
		}
		return -1L;
	}

	private static long findNextLineBreak(BigMappedByteBuffer buffer, long from, long end) {
		for (long i = from; i < end; i++) {
			byte b = byteAt(buffer, i);
			if (b == LF) {
				return i;
			}
			if (b == CR) {
				if (i + 1 < end && byteAt(buffer, i + 1) == LF) {
					return i + 1;
				}
				return i;
			}
		}
		return end;
	}

	private static long findNextUnescapedLineBreakEnd(BigMappedByteBuffer buffer, long from, long fileSize) {
		long i = from;
		while (i < fileSize) {
			if (byteAt(buffer, i) == LF) {
				long newlineStart = i;
				if (i > 0 && byteAt(buffer, i - 1) == CR) {
					newlineStart = i - 1;
				}
				if (!isEscaped(buffer, newlineStart)) {
					return i + 1;
				}
			}
			i++;
		}
		return fileSize;
	}

	private static boolean isEscaped(BigMappedByteBuffer buffer, long newlineStart) {
		long j = newlineStart - 1;
		int backslashes = 0;
		while (j >= 0 && byteAt(buffer, j) == BS) {
			backslashes++;
			j--;
		}
		return (backslashes & 1) == 1;
	}

	private static byte byteAt(BigMappedByteBuffer buffer, long offset) {
		return buffer.get(offset);
	}

	private static String segmentToString(BigMappedByteBuffer buffer, long start, long endInclusive) {
		int len = Math.toIntExact(endInclusive - start + 1);
		byte[] data = new byte[len];
		for (int i = 0; i < len; i++) {
			data[i] = buffer.get(start + i);
		}
		return new String(data, StandardCharsets.UTF_8);
	}

	private static String segmentToString(char[] buffer, int start, int endInclusive) {
		int len = endInclusive - start + 1;
		return new String(buffer, start, len);
	}
}

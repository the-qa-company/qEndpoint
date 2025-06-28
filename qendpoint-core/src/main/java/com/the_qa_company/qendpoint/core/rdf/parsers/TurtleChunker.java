package com.the_qa_company.qendpoint.core.rdf.parsers;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class TurtleChunker {

	private int consecutiveBackslashes;
	private boolean prefixOrBase;

	private enum State {
		DEFAULT, PERIOD_PENDING, IRI, LITERAL, MULTILINE_LITERAL, LANG_TAG_OR_DATATYPE, PREFIX_OR_BASE,
		CONSUME_WHITESPACE
	}

	private State state = State.DEFAULT;

	private static final int BUFFER_SIZE = 1024 * 1024 * 4;

	private final InputStream in; // CHANGED (was Reader)
	private final byte[] chunkBuf = new byte[BUFFER_SIZE];
	private int bufPos = 0, bufLen = 0;

	/**
	 * Stores partial bytes if the block spans multiple reads. If we never
	 * refill mid-block, we won't need this.
	 */
	private final ByteArrayOutputStream partialBytes = new ByteArrayOutputStream(); // CHANGED

	private final Deque<Character> nestingStack = new ArrayDeque<>();
	private byte literalDelimiter;

	private final MethodHandle[] defaultActions = new MethodHandle[256];
	private String finishedOneBlock = null;

	/**
	 * Indicates whether the current block has already crossed multiple reads
	 * (thus is partially in `partialBytes`).
	 */
	private boolean multiReadBlock = false; // CHANGED
	/**
	 * Marks where the current block started in `chunkBuf` if not in multi-read
	 * mode.
	 */
	private int chunkStart = 0; // CHANGED

	public TurtleChunker(InputStream in) { // CHANGED
		this.in = in;
		buildDefaultActions();
	}

	private void buildDefaultActions() {
		try {
			MethodHandles.Lookup lookup = MethodHandles.lookup();
			MethodType rawMt = MethodType.methodType(void.class, byte.class);

			MethodHandle rawLt = lookup.findVirtual(TurtleChunker.class, "handleLtInDefault", rawMt);
			MethodHandle rawHash = lookup.findVirtual(TurtleChunker.class, "handleHashInDefault", rawMt);
			MethodHandle rawLParen = lookup.findVirtual(TurtleChunker.class, "handleLParenInDefault", rawMt);
			MethodHandle rawRParen = lookup.findVirtual(TurtleChunker.class, "handleRParenInDefault", rawMt);
			MethodHandle rawLBrack = lookup.findVirtual(TurtleChunker.class, "handleLBrackInDefault", rawMt);
			MethodHandle rawRBrack = lookup.findVirtual(TurtleChunker.class, "handleRBrackInDefault", rawMt);
			MethodHandle rawQ1 = lookup.findVirtual(TurtleChunker.class, "handleQuote1InDefault", rawMt);
			MethodHandle rawQ2 = lookup.findVirtual(TurtleChunker.class, "handleQuote2InDefault", rawMt);
			MethodHandle rawDot = lookup.findVirtual(TurtleChunker.class, "handleDotInDefault", rawMt);
			MethodHandle rawBackslash = lookup.findVirtual(TurtleChunker.class, "handleBackslashInDefault", rawMt);
			MethodHandle rawAt = lookup.findVirtual(TurtleChunker.class, "handleAtInDefault", rawMt);

			MethodHandle boundLt = rawLt.bindTo(this);
			MethodHandle boundHash = rawHash.bindTo(this);
			MethodHandle boundLParen = rawLParen.bindTo(this);
			MethodHandle boundRParen = rawRParen.bindTo(this);
			MethodHandle boundLBrack = rawLBrack.bindTo(this);
			MethodHandle boundRBrack = rawRBrack.bindTo(this);
			MethodHandle boundQ1 = rawQ1.bindTo(this);
			MethodHandle boundQ2 = rawQ2.bindTo(this);
			MethodHandle boundDot = rawDot.bindTo(this);
			MethodHandle boundBackslash = rawBackslash.bindTo(this);
			MethodHandle boundAt = rawAt.bindTo(this);

			defaultActions['<'] = boundLt;
			defaultActions['#'] = boundHash;
			defaultActions['('] = boundLParen;
			defaultActions[')'] = boundRParen;
			defaultActions['['] = boundLBrack;
			defaultActions[']'] = boundRBrack;
			defaultActions['\''] = boundQ1;
			defaultActions['"'] = boundQ2;
			defaultActions['.'] = boundDot;
			defaultActions['\\'] = boundBackslash;
			defaultActions['@'] = boundAt;

		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new RuntimeException("Failed to build defaultActions", e);
		}
	}

	/*
	 * ---------------------------------------------------------------- The main
	 * loop that reads & parses blocks.
	 * ----------------------------------------------------------------
	 */
	private String parseNextBlock() throws IOException {
		while (true) {
			if (bufPos >= bufLen) {
				readMoreData();
			}
			if (bufLen == 0) {
				// no more data => produce leftover partial if any
				if (partialBytes.size() > 0) { // CHANGED
					partialBytes.reset(); // CHANGED
					// CHANGED
					String leftoverStr = partialBytes.toString(StandardCharsets.UTF_8);
					leftoverStr = leftoverStr.trim();
					return leftoverStr.isEmpty() ? null : leftoverStr;
				}
				return null; // truly no more
			}
			switch (state) {
			case DEFAULT -> parseDefaultOneStep();
			case PERIOD_PENDING -> parsePeriodOneStep();
			case IRI -> parseIriOneStep();
			case LITERAL -> parseLiteralOneStep();
			case MULTILINE_LITERAL -> parseMultilineLiteralOneStep();
			case LANG_TAG_OR_DATATYPE -> parseLangTagOrDatatypeOneStep();
			case PREFIX_OR_BASE -> parsePrefixOrBaseOneStep();
			case CONSUME_WHITESPACE -> {
				// This state is not used in the current implementation.
				// It can be used to handle whitespace between tokens.
				// For now, we just skip it.
				if (Character.isWhitespace(chunkBuf[bufPos])) {
					chunkStart++;
					bufPos++;
				} else {
					// If we encounter a non-whitespace character, we need to
					// handle it.
					// We can either transition to the DEFAULT state or handle
					// it as a special case.
					state = State.DEFAULT;
				}
			}
			}
			if (finishedOneBlock != null) {
				String block = finishedOneBlock;
				finishedOneBlock = null;
				state = State.CONSUME_WHITESPACE;
				return block;
			}
		}
	}

	private void parsePrefixOrBaseOneStep() {
		byte b = nextByte();
		if (b != 'p' && b != 'b') {
			throw new RuntimeException("Expected 'p' or 'b' after '@', but got: " + (char) b);
		}
		this.prefixOrBase = true;
		this.state = State.DEFAULT;
	}

	/*
	 * ----------------------------------------------------------------
	 * parseXxxOneStep methods: We do not append to partialBytes here unless we
	 * are finalizing the block. We parse in-place from chunkBuf for ASCII
	 * triggers, etc.
	 * ----------------------------------------------------------------
	 */

	private void parseDefaultOneStep() throws IOException {
		byte b = nextByte();
		MethodHandle mh = defaultActions[b & 0xFF];
		if (mh != null) {
			try {
				mh.invokeExact(b); // (byte)->void
			} catch (Throwable t) {
				if (t instanceof IOException ioException) {
					throw ioException;
				} else if (t instanceof Error error) {
					throw error;
				} else if (t instanceof RuntimeException runtimeException) {
					throw runtimeException;
				} else if (t instanceof InterruptedException) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(t);
				}
				throw new RuntimeException(t);
			}
		}
	}

	private void parseIriOneStep() {
		while (bufPos < bufLen) {
			byte b = nextByte();
			if (b == '>') {
				state = State.DEFAULT;
				return;
			}
		}
	}

	private void parseLiteralOneStep() {
		while (bufPos < bufLen) {
			byte b = nextByte();
			if (b == '\\') {
				consecutiveBackslashes++;
				continue;
			}
			boolean escaped = (consecutiveBackslashes % 2 == 1);
			consecutiveBackslashes = 0; // reset whenever we see a non-backslash

			if (b == literalDelimiter && !escaped) {
				state = State.LANG_TAG_OR_DATATYPE;
				return;
			}
		}
	}

	private void parseMultilineLiteralOneStep() throws IOException {

		while (bufPos < bufLen) {
			byte b = nextByte();

			if (b == '\\') {
				consecutiveBackslashes++;
				continue;
			}

			boolean escaped = (consecutiveBackslashes % 2 == 1);
			consecutiveBackslashes = 0; // reset whenever we see a non-backslash

			if (b == literalDelimiter && !escaped) {
				if (checkForTripleQuote(literalDelimiter)) {
					state = State.LANG_TAG_OR_DATATYPE;
					return;
				}
			}
		}
	}

	private void parseLangTagOrDatatypeOneStep() throws IOException {
		byte b = chunkBuf[bufPos];
		// We do NOT consume it yet in case we want to pass it to default action

		if (b == '@') {
			// We detected a language tag start. For now, you said we don't
			// parse it fully,
			// so you might just consume the '@'

			bufPos++; // consume '@'

			state = State.DEFAULT;
		} else if (b == '^') {
			// Could be start of ^^<datatype>
			bufPos++; // consume '^'
			state = State.DEFAULT;
		} else {
			// Not '@' or '^', so let's pass it through to the defaultAction
			// logic.
			state = State.DEFAULT;
		}
	}

	/*
	 * ---------------------------------------------------------------- Special
	 * char handlers in DEFAULT state
	 * ----------------------------------------------------------------
	 */

	private void handleLtInDefault(byte b) {
		state = State.IRI;
	}

	private void handleHashInDefault(byte b) {
		skipComment();
	}

	private void handleLParenInDefault(byte b) {
		nestingStack.push('(');
	}

	private void handleRParenInDefault(byte b) {
		if (!nestingStack.isEmpty()) {
			nestingStack.pop();
		}
	}

	private void handleLBrackInDefault(byte b) {
		nestingStack.push('[');
	}

	private void handleRBrackInDefault(byte b) {
		if (!nestingStack.isEmpty()) {
			nestingStack.pop();
		}
	}

	private void handleQuote1InDefault(byte b) throws IOException {
		if (checkForTripleQuote(b)) {
			state = State.MULTILINE_LITERAL;
			literalDelimiter = b;
		} else {
			state = State.LITERAL;
			literalDelimiter = b;
		}
	}

	private void handleQuote2InDefault(byte b) throws IOException {
		if (checkForTripleQuote(b)) {
			state = State.MULTILINE_LITERAL;
			literalDelimiter = b;
		} else {
			state = State.LITERAL;
			literalDelimiter = b;
		}
	}

	// Modified: Instead of finalizing directly, we transition to
	// PERIOD_PENDING.
	private void handleDotInDefault(byte b) {
		if (nestingStack.isEmpty()) {
			state = State.PERIOD_PENDING;
		}
	}

	private void handleBackslashInDefault(byte b) throws IOException {
		// b is the backslash we've just encountered.
		// Let's skip to the next byte if we're not at the end of the buffer.
		// The next byte is escaped, so we don't need to check it.
		if (bufPos >= bufLen) {
			readMoreData();
		}
		if (bufPos < bufLen) {
			nextByte(); // consume and discard the next byte
		}
	}

	private void handleAtInDefault(byte b) throws IOException {
		if (bufPos - chunkStart > 1) {
			throw new RuntimeException("Unexpected @ in block: "
					+ new String(chunkBuf, chunkStart, bufPos - chunkStart, StandardCharsets.UTF_8));
		} else {
			state = State.PREFIX_OR_BASE;
		}
	}

	private void parsePeriodOneStep() {
		// We assume bufPos < bufLen due to the check in parseNextBlock().
		byte next = chunkBuf[bufPos];
		if (next == ' ' || next == '\t' || next == '\n' || next == '\r') {
			state = State.DEFAULT;
			finalizeBlock();
		} else {
			state = State.DEFAULT;
		}
	}

	/*
	 * ----------------------------------------------------------------
	 * finalizeBlock: build the final statement string
	 * ----------------------------------------------------------------
	 */
	private void finalizeBlock() {
		if (!multiReadBlock) {
			// The entire block is in chunkBuf from chunkStart..bufPos
			int length = bufPos - chunkStart;
			if (length <= 0) {
				return; // nothing
			}
			String block = new String(chunkBuf, chunkStart, length, StandardCharsets.UTF_8);

			chunkStart = bufPos; // next block starts here
			finishedOneBlock = block;
		} else {
			// partial data is in partialBytes + leftover in chunkBuf
			if (bufPos > chunkStart) {
				partialBytes.write(chunkBuf, chunkStart, (bufPos - chunkStart)); // CHANGED
			}
			String block = partialBytes.toString(StandardCharsets.UTF_8);

			partialBytes.reset(); // CHANGED
			finishedOneBlock = block;
			multiReadBlock = false;
			chunkStart = bufPos;
		}
	}

	/*
	 * ----------------------------------------------------------------
	 * skipComment, tripleQuote, escaping checks We parse in place for
	 * detection.
	 * ----------------------------------------------------------------
	 */

	private void skipComment() {
		while (true) {
			if (bufPos >= bufLen) {
				return;
			}
			byte b = nextByte();
			// check if the byte represents an ASCII character, if not then it's
			// not relevant to check
			if ((b & 0x80) != 0) {
				continue;
			}

			if (b == '\n') {
				return;
			}
		}
	}

	private byte nextByte() {
		return chunkBuf[bufPos++];
	}

	private boolean checkForTripleQuote(byte quoteChar) throws IOException {
		if (bufPos >= bufLen) {
			readMoreData();
		}

		if (bufPos >= bufLen) {
			return false;
		}

		if (chunkBuf[bufPos] == quoteChar) {
			bufPos++;
			if (bufPos >= bufLen) {
				readMoreData();
			}
			if (bufPos < bufLen) {
				if (chunkBuf[bufPos] == quoteChar) {
					bufPos++;
					return true;
				}
				return false;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	/*
	 * ----------------------------------------------------------------
	 * readMoreData: if we run out of data & haven't ended the block, copy
	 * leftover from chunkBuf to partialBytes to avoid overwriting it.
	 * ----------------------------------------------------------------
	 */
	private void readMoreData() throws IOException {
		// If we haven't finished the current block
		if (chunkStart < bufLen) {
			partialBytes.write(chunkBuf, chunkStart, bufLen - chunkStart); // CHANGED
			multiReadBlock = true;
		}
		chunkStart = 0;
		bufLen = in.read(chunkBuf);
		bufPos = 0;
		if (bufLen == -1) {
			bufLen = 0; // EOF
		}
	}

	/*
	 * ----------------------------------------------------------------
	 * BlockIterator
	 * ----------------------------------------------------------------
	 */

	public BlockIterator blockIterator() {
		return new BlockIterator();
	}

	public class BlockIterator implements Iterator<String> {
		private String nextBlock;
		private boolean done;
		Consumer<String> prefixConsumer = null;

		public void setPrefixConsumer(Consumer<String> prefixConsumer) {
			this.prefixConsumer = prefixConsumer;
		}

		String getPrefixes() {
			StringBuilder sb = new StringBuilder();
//			while (hasNext()) {
//				String lowerCase = nextBlock.trim().toLowerCase();
//				if (lowerCase.isEmpty() || lowerCase.startsWith("#")) {
//					nextBlock = null;
//				} else if (lowerCase.startsWith("@prefix") || lowerCase.startsWith("@base")) {
//					sb.append(nextBlock.trim()).append("\n");
//					nextBlock = null;
//				} else {
//					break;
//				}
//			}
			return sb.toString();
		}

		@Override
		public boolean hasNext() {
			if (done) {
				return false;
			}
			if (nextBlock != null) {
				return true;
			}
			while (true) {
				try {
					nextBlock = parseNextBlock();
					if (prefixOrBase) {
						this.prefixConsumer.accept(nextBlock);
						prefixOrBase = false;
						nextBlock = null;
					} else {
						if (nextBlock == null) {
							done = true;
							return false;
						}
						return true;
					}

				} catch (IOException e) {
					done = true;
					throw new RuntimeException("IO error during iteration", e);
				}
			}

		}

		@Override
		public String next() {
			if (!hasNext()) {
				throw new NoSuchElementException("No more blocks");
			}
			String result = nextBlock;
			nextBlock = null;
			return result;
		}
	}

	// -- Example main for testing --
	public static void main(String[] args) {
		String filePath = "/Users/havardottestad/Documents/Programming/qEndpoint3/indexing/latest-dump.ttl"; // Update
		// path
		long actualStart = System.currentTimeMillis();
		long start = System.currentTimeMillis();
		long count = 0;
		long total = 0;

		try (InputStream sr = new FileInputStream(filePath)) {
			TurtleChunker tokenizer = new TurtleChunker(sr);
			BlockIterator it = tokenizer.blockIterator();

			System.out.println("Processing with NIO AsynchronousFileChannel (blocking wait)...");

			it.setPrefixConsumer(p -> System.out.println("Prefix: " + p));

			while (it.hasNext()) {
				String block = it.next();
				int length = block.trim().split("\n").length;
				count += length;
				total += length;
				if (count > 10_000_000) {
					System.out.println(count + " lines parsed");
					System.out.println(block);
					System.out.printf("Lines per second: %,d \n", count * 1000 / (System.currentTimeMillis() - start));
					System.out.printf("Lines per second (total): %,d \n",
							total * 1000 / (System.currentTimeMillis() - actualStart));

					start = System.currentTimeMillis();
					count = 0;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			long actualEnd = System.currentTimeMillis();
			long total2 = actualEnd - actualStart;
			long minutes = total2 / 60000;
			long seconds = (total2 % 60000) / 1000;
			System.out.printf("Total: %,d \n", total);
			System.out.printf("Total time: %d:%02d%n", minutes, seconds);
		}
	}
}

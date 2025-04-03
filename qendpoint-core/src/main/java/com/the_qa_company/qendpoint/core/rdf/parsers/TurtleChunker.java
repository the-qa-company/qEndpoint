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

/**
 * A preprocessor for Turtle files, which reads the file and returns an iterator
 * of chunks where each chunk is a block of turtle data with a . termination.
 * This might be a single statement, or multiple statements.
 */
public class TurtleChunker {

	private int consecutiveBackslashes;

	private enum State {
		DEFAULT, IRI, LITERAL, MULTILINE_LITERAL
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

			MethodHandle boundLt = rawLt.bindTo(this);
			MethodHandle boundHash = rawHash.bindTo(this);
			MethodHandle boundLParen = rawLParen.bindTo(this);
			MethodHandle boundRParen = rawRParen.bindTo(this);
			MethodHandle boundLBrack = rawLBrack.bindTo(this);
			MethodHandle boundRBrack = rawRBrack.bindTo(this);
			MethodHandle boundQ1 = rawQ1.bindTo(this);
			MethodHandle boundQ2 = rawQ2.bindTo(this);
			MethodHandle boundDot = rawDot.bindTo(this);

			defaultActions['<'] = boundLt;
			defaultActions['#'] = boundHash;
			defaultActions['('] = boundLParen;
			defaultActions[')'] = boundRParen;
			defaultActions['['] = boundLBrack;
			defaultActions[']'] = boundRBrack;
			defaultActions['\''] = boundQ1;
			defaultActions['"'] = boundQ2;
			defaultActions['.'] = boundDot;
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
			case IRI -> parseIriOneStep();
			case LITERAL -> parseLiteralOneStep();
			case MULTILINE_LITERAL -> parseMultilineLiteralOneStep();
			}

			if (finishedOneBlock != null) {
				String block = finishedOneBlock;
				finishedOneBlock = null;
				return block;
			}
		}
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
				state = State.DEFAULT;
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
					state = State.DEFAULT;
					return;
				}
			}
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

	private void handleDotInDefault(byte b) {
		if (nestingStack.isEmpty()) {
			finalizeBlock();
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

		String getPrefixes() {
			StringBuilder sb = new StringBuilder();
			while (hasNext()) {
				String lowerCase = nextBlock.trim().toLowerCase();
				if (lowerCase.isEmpty() || lowerCase.startsWith("#")) {
					nextBlock = null;
				} else if (lowerCase.startsWith("@prefix") || lowerCase.startsWith("@base")) {
					sb.append(nextBlock.trim()).append("\n");
					nextBlock = null;
				} else {
					break;
				}
			}
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
			try {
				nextBlock = parseNextBlock();
				if (nextBlock == null) {
					done = true;
					return false;
				}
				return true;
			} catch (IOException e) {
				done = true;
				throw new RuntimeException("IO error during iteration", e);
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

		try (InputStream sr = new FileInputStream(
				"/Users/havardottestad/Documents/Programming/qEndpoint3/indexing/latest-dump.ttl")) {
			TurtleChunker tokenizer = new TurtleChunker(sr);
			BlockIterator it = tokenizer.blockIterator();

			System.out.println("Processing with NIO AsynchronousFileChannel (blocking wait)...");

			String prefixes = it.getPrefixes();
			System.out.println("Prefixes:\n" + prefixes);

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

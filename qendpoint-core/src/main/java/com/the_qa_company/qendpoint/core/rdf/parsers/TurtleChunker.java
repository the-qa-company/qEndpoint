package com.the_qa_company.qendpoint.core.rdf.parsers;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
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

	private enum State {
		DEFAULT, IRI, LITERAL, MULTILINE_LITERAL
	}

	private State state = State.DEFAULT;

	private static final int BUFFER_SIZE = 1024 * 1024 * 4;
	private final Reader reader;
	private final char[] chunkBuf = new char[BUFFER_SIZE];
	private int bufPos = 0, bufLen = 0;

	private final StringBuilder tokenBuffer = new StringBuilder();

	private final Deque<Character> nestingStack = new ArrayDeque<>();

	private char literalDelimiter;

	private final MethodHandle[] defaultActions = new MethodHandle[256];

	public TurtleChunker(Reader reader) {
		this.reader = reader;
		buildDefaultActions();
	}

	private void buildDefaultActions() {
		try {
			MethodHandles.Lookup lookup = MethodHandles.lookup();

			MethodType rawMt = MethodType.methodType(void.class, char.class);

			MethodHandle rawLt = lookup.findVirtual(TurtleChunker.class, "handleLtInDefault", rawMt);
			MethodHandle rawHash = lookup.findVirtual(TurtleChunker.class, "handleHashInDefault", rawMt);

			MethodHandle rawLParen = lookup.findVirtual(TurtleChunker.class, "handleLParenInDefault", rawMt);
			MethodHandle rawRParen = lookup.findVirtual(TurtleChunker.class, "handleRParenInDefault", rawMt);
			MethodHandle rawLBrack = lookup.findVirtual(TurtleChunker.class, "handleLBrackInDefault", rawMt);
			MethodHandle rawRBrack = lookup.findVirtual(TurtleChunker.class, "handleRBrackInDefault", rawMt);
			MethodHandle rawQ1 = lookup.findVirtual(TurtleChunker.class, "handleQuote1InDefault", rawMt);
			MethodHandle rawQ2 = lookup.findVirtual(TurtleChunker.class, "handleQuote2InDefault", rawMt);
			MethodHandle rawDot = lookup.findVirtual(TurtleChunker.class, "handleDotInDefault", rawMt);

			// Bind each method to `this` so it becomes (char)->void
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

	private String parseNextBlock() throws IOException {
		while (true) {
			refillIfNeeded();
			if (bufLen == 0) {
				// no more data => produce any leftover partial
				if (!tokenBuffer.isEmpty()) {
					// Possibly return a final partial statement
					String leftover = tokenBuffer.toString().trim();
					tokenBuffer.setLength(0);
					return leftover.isEmpty() ? null : leftover;
				}
				return null; // truly no more
			}

			switch (state) {
				case DEFAULT -> parseDefaultOneStep();
				case IRI -> parseIriOneStep();
				case LITERAL -> parseLiteralOneStep();
				case MULTILINE_LITERAL -> parseMultilineLiteralOneStep();
			}

			// Check if we completed a "block"?
			// The condition: parseDefault encountered '.' outside nesting =>
			// calls completeToken()
			if (finishedOneBlock != null) {
				String block = finishedOneBlock;
				finishedOneBlock = null;
				return block;
			}
		}
	}

	private String finishedOneBlock = null;

	private void parseDefaultOneStep() {
		char ch = chunkBuf[bufPos++];
		tokenBuffer.append(ch);

		MethodHandle mh = defaultActions[ch & 0xFF];
		if (mh != null) {
			try {
				mh.invokeExact(ch); // (char)->void
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
		}

	}

	private void parseIriOneStep() {
		char ch = chunkBuf[bufPos];
		tokenBuffer.append(ch);
		bufPos++;
		if (ch == '>') {
			state = State.DEFAULT;
		}

	}

	private void parseLiteralOneStep() {
		char ch = chunkBuf[bufPos++];
		tokenBuffer.append(ch);

		if (ch == literalDelimiter && !isEscaped()) {
			state = State.DEFAULT;
		}
	}

	private void parseMultilineLiteralOneStep() {
		char ch = chunkBuf[bufPos++];
		tokenBuffer.append(ch);

		if (ch == literalDelimiter && !isEscaped()) {
			if (maybeEndTripleQuote()) {
				state = State.DEFAULT;
			}
		}
	}

	private void handleLtInDefault(char ch) {
		state = State.IRI;
	}

	private void handleHashInDefault(char ch) {
		skipComment();
	}

	private void handleLParenInDefault(char ch) {
		nestingStack.push('(');
	}

	private void handleRParenInDefault(char ch) {
		if (!nestingStack.isEmpty()) {
			nestingStack.pop();
		}
	}

	private void handleLBrackInDefault(char ch) {
		nestingStack.push('[');
	}

	private void handleRBrackInDefault(char ch) {
		if (!nestingStack.isEmpty()) {
			nestingStack.pop();
		}
	}

	private void handleQuote1InDefault(char ch) {
		if (checkForTripleQuote(ch)) {
			state = State.MULTILINE_LITERAL;
			literalDelimiter = ch;
		} else {
			state = State.LITERAL;
			literalDelimiter = ch;
		}
	}

	private void handleQuote2InDefault(char ch) {
		if (checkForTripleQuote(ch)) {
			state = State.MULTILINE_LITERAL;
			literalDelimiter = ch;
		} else {
			state = State.LITERAL;
			literalDelimiter = ch;
		}
	}

	private void handleDotInDefault(char ch) {
		if (nestingStack.isEmpty()) {
			String block = tokenBuffer.toString();
			if (tokenBuffer.capacity() > 512 * 1024) {
				tokenBuffer.setLength(0);
				tokenBuffer.trimToSize();
			} else {
				tokenBuffer.setLength(0);
			}
			if (!block.isEmpty()) {
				finishedOneBlock = block;
			}
		}
	}

	private void skipComment() {
		while (true) {
			if (bufPos >= bufLen) {
				return;
			}
			char ch = chunkBuf[bufPos++];
			tokenBuffer.append(ch);
			if (ch == '\n') {
				return;
			}
		}
	}

	private boolean checkForTripleQuote(char quoteChar) {
		if (bufPos + 1 < bufLen) {
			if (chunkBuf[bufPos] == quoteChar && chunkBuf[bufPos + 1] == quoteChar) {
				tokenBuffer.append(chunkBuf[bufPos++]);
				tokenBuffer.append(chunkBuf[bufPos++]);
				return true;
			}
		}
		return false;
	}

	private boolean maybeEndTripleQuote() {
		if (bufPos + 1 < bufLen) {
			if (chunkBuf[bufPos] == literalDelimiter && chunkBuf[bufPos + 1] == literalDelimiter) {
				tokenBuffer.append(chunkBuf[bufPos++]);
				tokenBuffer.append(chunkBuf[bufPos++]);
				return true;
			}
		}
		return false;
	}

	private boolean isEscaped() {
		int len = tokenBuffer.length();
		int backslashCount = 0;
		for (int i = len - 2; i >= 0; i--) {
			if (tokenBuffer.charAt(i) == '\\') {
				backslashCount++;
			} else {
				break;
			}
		}
		return (backslashCount % 2 == 1);
	}

	private void refillIfNeeded() throws IOException {
		while (bufPos >= bufLen) {
			bufLen = reader.read(chunkBuf);
			bufPos = 0;
			if (bufLen == -1) {
				bufLen = 0; // EOF
				return;
			}
		}
	}

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

	public static void main(String[] args) throws FileNotFoundException {

		long actualStart = System.currentTimeMillis();

		long start = System.currentTimeMillis();
		long count = 0;

		try (Reader sr = (new FileReader("/Users/havardottestad/Downloads/aria2c/latest-dump.ttl"))) {
			TurtleChunker tokenizer = new TurtleChunker(sr);
			BlockIterator it = tokenizer.blockIterator();

			String prefixes = it.getPrefixes();
			System.out.println("Prefixes:\n" + prefixes);

			while (it.hasNext()) {
				String block = it.next();
				count += block.trim().split("\n").length;
				if (count > 10000000) {
					System.out.println(count + " lines parsed");
					System.out.println(block);
					// count per second with thousands separator
					System.out.printf("Lines per second: %,d \n", count * 1000 / (System.currentTimeMillis() - start));
					start = System.currentTimeMillis();
					count = 0;
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			long actualEnd = System.currentTimeMillis();
			long total = actualEnd - actualStart;

			long minutes = total / 60000;
			long seconds = (total % 60000) / 1000;
			System.out.printf("Total time: %d:%02d%n", minutes, seconds);

		}
	}
}

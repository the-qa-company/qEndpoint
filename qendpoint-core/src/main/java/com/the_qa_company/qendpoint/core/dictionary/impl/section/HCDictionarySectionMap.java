package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.Sequence;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceFactory;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.dictionary.TempDictionarySection;
import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.exceptions.IllegalFormatException;
import com.the_qa_company.qendpoint.core.exceptions.NotImplementedException;
import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBuffer;
import com.the_qa_company.qendpoint.core.util.io.BigMappedByteBufferInputStream;
import com.the_qa_company.qendpoint.core.util.io.ByteBufferInputStream;
import com.the_qa_company.qendpoint.core.util.io.Closer;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import com.the_qa_company.qendpoint.core.util.string.ReplazableString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Iterator;

public class HCDictionarySectionMap implements DictionarySectionPrivate {

	private static final Logger log = LoggerFactory.getLogger(HCDictionarySectionMap.class);

	public static final int TYPE_INDEX = 0x31;
	public static final long BLOCKS_PER_BYTEBUFFER = 50_000;
	public static final long HASHES_PER_BLOCK = 100_000;

	private final File f;
	private final long startOffset;
	private final long endOffset;

	protected FileChannel ch;
	protected BigMappedByteBuffer[] buffers; // Encoded sequence
	protected Sequence blocks;
	protected Sequence hashes;
	protected long[] posFirst;
	protected long[] hashesPos;
	protected int blocksize;
	protected long numstrings;
	protected long dataSize;

	public HCDictionarySectionMap(CountInputStream input, File f) throws IOException {
		this.f = f;
		startOffset = input.getTotalBytes();

		CRCInputStream crcin = new CRCInputStream(input, new CRC8());

		int type = crcin.read();
		if (type != HCDictionarySectionMap.TYPE_INDEX) {
			throw new IllegalFormatException(
					"Trying to read a HCDictionarySection from data that is not of the suitable type");
		}

		// Read vars
		numstrings = VByte.decode(crcin);
		dataSize = VByte.decode(crcin);
		blocksize = (int) VByte.decode(crcin);

		if (!crcin.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}

		// Read blocks
		blocks = SequenceFactory.createStream(input, f);
		hashes = SequenceFactory.createStream(input, f);

		long base = input.getTotalBytes();
		IOUtil.skip(crcin, dataSize + 4); // Including CRC32

		endOffset = input.getTotalBytes();

		// Read packed data
		ch = FileChannel.open(Paths.get(f.toString()));
		long block = 0;
		int buffer = 0;
		long numBlocks = blocks.getNumberOfElements();
		long bytePos = 0;
		long numBuffers = 1L + numBlocks / BLOCKS_PER_BYTEBUFFER;
		buffers = new BigMappedByteBuffer[(int) numBuffers];
		posFirst = new long[(int) numBuffers];

		while (block < numBlocks - 1) {
			long nextBlock = Math.min(numBlocks - 1, block + BLOCKS_PER_BYTEBUFFER);
			long nextBytePos = blocks.get(nextBlock);

			buffers[buffer] = BigMappedByteBuffer.ofFileChannel(f.getAbsolutePath(), ch, FileChannel.MapMode.READ_ONLY,
					base + bytePos, nextBytePos - bytePos);
			buffers[buffer].order(ByteOrder.LITTLE_ENDIAN);

			posFirst[buffer] = bytePos;

			bytePos = nextBytePos;
			block += BLOCKS_PER_BYTEBUFFER;
			buffer++;
		}

		int numHashBlocks = (int) (numstrings / HASHES_PER_BLOCK);

		hashesPos = new long[numHashBlocks + 1];

		// preload some hashes to reduce the map impact when doing the search
		for (int i = 0; i < numHashBlocks; i++) {
			hashesPos[i] = hashes.get(Math.min(i * HASHES_PER_BLOCK, numstrings - 1));
		}
		if (numstrings % HASHES_PER_BLOCK == 0) {
			hashesPos[numHashBlocks] = hashes.get(numstrings - 1);
		} else {
			hashesPos[numHashBlocks] = hashes.get(numstrings - 1) + 1;
		}
	}

	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		load(other.getEntries(), other.getNumberOfElements(), listener);
	}

	@Override
	public void load(Iterator<? extends CharSequence> it, long count, ProgressListener listener) {
		throw new NotImplementedException("load");
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		throw new NotImplementedException("save");

	}

	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		throw new NotImplementedException("load");
	}

	@Override
	public long locate(CharSequence s) {
		ByteString bs = ByteString.of(s);
		long hash = bs.hash39();

		return 0;
	}

	@Override
	public CharSequence extract(long id) {
		if (buffers == null || blocks == null) {
			return null;
		}

		if (id < 1 || id > numstrings) {
			return null;
		}

		long block = (id - 1) / blocksize;
		BigMappedByteBuffer buffer = buffers[(int) (block / BLOCKS_PER_BYTEBUFFER)].duplicate();
		buffer.position(blocks.get(block) - posFirst[(int) (block / BLOCKS_PER_BYTEBUFFER)]);

		try {
			int blockLen = (int)VByte.decode(buffer);
			ZstdInputStream is = new ZstdInputStream(new BigMappedByteBufferInputStream(buffer, buffer.position(), blockLen));

			long stringid = (id - 1) % blocksize;
			// skip the previous strings
			for (long i = 1; i < stringid; i++) {
				long len = VByte.decode(is);
				is.skipNBytes(len);
			}
			// read str
			int len = (int) VByte.decode(is);
			byte[] strbuff = new byte[len];
			if (is.readNBytes(strbuff, 0, len) != len) {
				throw new IOException("Can't decode string");
			}
			return new CompactString(strbuff).getDelayed();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long size() {
		return dataSize + blocks.size() + hashes.size();
	}

	@Override
	public long getNumberOfElements() {
		return numstrings;
	}

	@Override
	public Iterator<? extends CharSequence> getSortedEntries() {
		return new FetcherIterator<CharSequence>() {
			private ReplazableString tempString = new ReplazableString();
			private ZstdInputStream zstdstream;
			private long offset;
			private long strid;

			@Override
			protected CharSequence getNext() {
				if (strid >= numstrings) return null;
				if (strid % blocksize == 0) {
					// need to load the stream


					
				}
				strid++;
				return null;
			}
		};
	}

	@Override
	public void close() throws IOException {
		if (buffers != null) {
			for (BigMappedByteBuffer buffer : buffers) {
				if (buffer != null) {
					buffer.clean();
				}
			}
			buffers = null;
		}
		Closer.closeAll(hashes, blocks);
	}
}

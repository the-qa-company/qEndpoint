package com.the_qa_company.qendpoint.core.dictionary.impl.utilCat;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.compact.sequence.SequenceLog64BigDisk;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.listener.ListenerUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class SectionUtil {

	public static final ByteString S1 = ByteString.of("S1");
	public static final ByteString S2 = ByteString.of("S2");
	public static final ByteString P1 = ByteString.of("P1");
	public static final ByteString P2 = ByteString.of("P2");
	public static final ByteString O1 = ByteString.of("O1");
	public static final ByteString O2 = ByteString.of("O2");
	public static final ByteString SH1 = ByteString.of("SH1");
	public static final ByteString SH2 = ByteString.of("SH2");

	public static final ByteString SECTION = ByteString.of("section");
	public static final ByteString SECTION_SUBJECT = ByteString.of("subject");
	public static final ByteString SECTION_PREDICATE = ByteString.of("predicate");
	public static final ByteString SECTION_OBJECT = ByteString.of("object");
	public static final ByteString SECTION_SHARED = ByteString.of("shared");
	public static final ByteString BACK = ByteString.of("back");
	private static final ByteString SUB_PREFIX = ByteString.of("sub");

	private static final int DEFAULT_BLOCK_SIZE = 16;
	private static final int BLOCK_PER_BUFFER = 1000000;

	public static ByteString createSub(Object next) {
		return createSub(String.valueOf(next));
	}

	public static ByteString createSub(CharSequence next) {
		return createSub(ByteString.of(next));
	}

	public static ByteString createSub(ByteString next) {
		return SUB_PREFIX.copyAppend(next);
	}

	public static void createSection(String location, long numEntries, int type, CatUnion itAdd, CatUnion itSkip,
			Map<? extends CharSequence, CatMapping> mappings, long offset, ProgressListener listener)
			throws IOException {
		ByteString name;
		switch (type) {
		case 2:
			name = SECTION_SUBJECT;
			break;
		case 3:
			name = SECTION_OBJECT;
			break;
		case 4:
			name = SECTION_PREDICATE;
			break;
		default:
			name = ByteString.empty();
			break;
		}
		long storedBuffersSize = 0;
		long numBlocks = 0;
		long numberElements = 0;
		SequenceLog64BigDisk blocks;
		ByteArrayOutputStream byteOut;
		try (CRCOutputStream outBuffer = new CRCOutputStream(new FileOutputStream(location + "section_buffer_" + type),
				new CRC32())) {
			blocks = new SequenceLog64BigDisk(location + "SequenceLog64BigDisk" + type, 64, numEntries / 16);
			byteOut = new ByteArrayOutputStream(16 * 1024);
			if (numEntries > 0) {
				ByteString previousStr = null;

				CatElement skipElement = null;
				if (itSkip.hasNext()) {
					skipElement = itSkip.next();
				}
				while (itAdd.hasNext()) {
					ListenerUtil.notifyCond(listener, "Analyze section " + name + " ", numberElements, numberElements,
							numEntries);
					CatElement nextElement = itAdd.next();

					if (skipElement != null && nextElement.entity.equals(skipElement.entity)) {
						if (itSkip.hasNext()) {
							skipElement = itSkip.next();
						} else {
							skipElement = null;
						}
					} else {
						for (int i = 0; i < nextElement.IDs.size(); i++) {
							long id = nextElement.IDs.get(i).pos;
							ByteString iter = nextElement.IDs.get(i).iter;
							if (iter.equals(SECTION_SHARED))
								mappings.get(iter).set(id - 1, offset + numberElements + 1, type);
							else
								mappings.get(iter).set(id - 1, numberElements + 1, type);
						}
						ByteString str = nextElement.entity;
						if (numberElements % DEFAULT_BLOCK_SIZE == 0) {
							blocks.append(storedBuffersSize + byteOut.size());
							numBlocks++;

							// if a buffer is filled, flush the byteOut and
							// store it
							if (((numBlocks - 1) % BLOCK_PER_BUFFER == 0) && ((numBlocks - 1) / BLOCK_PER_BUFFER != 0)
									|| byteOut.size() > 200000) {
								storedBuffersSize += byteOut.size();
								byteOut.flush();
								byte[] arr = byteOut.toByteArray();
								IOUtil.writeBuffer(outBuffer, arr, 0, arr.length, null);
								byteOut.close();
								byteOut = new ByteArrayOutputStream(16 * 1024);
							}

							// Copy full string
							ByteStringUtil.append(byteOut, str, 0);
						} else {
							// Find common part.
							int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
							// Write Delta in VByte
							VByte.encode(byteOut, delta);
							// Write remaining
							ByteStringUtil.append(byteOut, str, delta);
						}
						byteOut.write(0); // End of string
						previousStr = str;
						numberElements += 1;
					}
				}
			}
			// Ending block pointer.
			blocks.append(storedBuffersSize + byteOut.size());
			// Trim text/blocks
			blocks.aggressiveTrimToSize();
			byteOut.flush();
			// section.addBuffer(buffer, byteOut.toByteArray());
			byte[] arr = byteOut.toByteArray();
			IOUtil.writeBuffer(outBuffer, arr, 0, arr.length, null);
			outBuffer.writeCRC();
		}
		// Save the section conforming to the HDT format
		try (CRCOutputStream out = new CRCOutputStream(new FileOutputStream(location + "section" + type), new CRC8())) {
			// write the index type
			out.write(2);
			// write the number of strings
			VByte.encode(out, numberElements);
			// write the datasize
			VByte.encode(out, storedBuffersSize + byteOut.size());
			// wirte the blocksize
			VByte.encode(out, DEFAULT_BLOCK_SIZE);
			// write CRC
			out.writeCRC();
			// write the blocks
			blocks.save(out, null); // Write blocks directly to output, they
									// have their own CRC check.
			blocks.close();
			// write out_buffer
			Files.copy(Path.of(location + "section_buffer_" + type), out);
		}

		Files.deleteIfExists(Paths.get(location + "section_buffer_" + type));
		Files.deleteIfExists(Paths.get(location + "SequenceLog64BigDisk" + type));
	}
}

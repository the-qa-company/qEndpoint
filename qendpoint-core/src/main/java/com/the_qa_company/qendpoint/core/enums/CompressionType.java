package com.the_qa_company.qendpoint.core.enums;

import com.the_qa_company.qendpoint.core.util.concurrent.ExceptionFunction;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A compression type
 *
 * @author Antoine Willerval
 */
public enum CompressionType {

	/**
	 * gzip compression (.gz .tgz)
	 */
	GZIP(GZIPInputStream::new, GZIPOutputStream::new, "gz", "tgz"),
	/**
	 * bzip compression (.bz2 .bz)
	 */
	BZIP(in -> new BZip2CompressorInputStream(in, true), BZip2CompressorOutputStream::new, "bz2", "bz"),
	/**
	 * xz compression (.xz)
	 */
	XZ(in -> new XZCompressorInputStream(in, true), XZCompressorOutputStream::new, "xz"),
	/**
	 * lz4 compression
	 */
	LZ4(LZ4FrameInputStream::new, LZ4FrameOutputStream::new),
	/**
	 * lz4 compression
	 */
	LZ4B(LZ4BlockInputStream::new, LZ4BlockOutputStream::new),
	/**
	 * lzma compression
	 */
	LZMA(LZMACompressorInputStream::new, LZMACompressorOutputStream::new),
	/**
	 * no compression
	 */
	NONE(ExceptionFunction.identity(), ExceptionFunction.identity());

	/**
	 * try to guess a compression of a file with its name
	 *
	 * @param fileName the file name to guess
	 * @return the compression type or none if it can't be guessed
	 */
	public static CompressionType guess(String fileName) {
		String str = fileName.toLowerCase();

		int idx = str.lastIndexOf('.');
		if (idx != -1) {
			String ext = str.substring(idx + 1);
			for (CompressionType type : values()) {
				for (String typeExt : type.ext) {
					if (typeExt.equals(ext)) {
						return type;
					}
				}
			}
		}
		return NONE;
	}

	public static CompressionType findOptionVal(String name) {
		if (name == null || name.isEmpty())
			return NONE;
		return valueOf(name.toUpperCase());
	}

	private final String[] ext;
	private final ExceptionFunction<InputStream, InputStream, IOException> decompress;
	private final ExceptionFunction<OutputStream, OutputStream, IOException> compress;

	CompressionType(ExceptionFunction<InputStream, InputStream, IOException> decompress,
			ExceptionFunction<OutputStream, OutputStream, IOException> compress, String... ext) {
		this.decompress = decompress;
		this.compress = compress;
		this.ext = ext;
		assert name().toUpperCase().equals(name()) : "bad name";
	}

	/**
	 * decompress a stream
	 *
	 * @param stream stream
	 * @return decompressed stream
	 * @throws IOException io
	 */
	public InputStream decompress(InputStream stream) throws IOException {
		return decompress.apply(stream);
	}

	/**
	 * compress a stream
	 *
	 * @param stream stream
	 * @return compressed stream
	 * @throws IOException io
	 */
	public OutputStream compress(OutputStream stream) throws IOException {
		return compress.apply(stream);
	}
}

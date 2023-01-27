package com.the_qa_company.qendpoint.core.enums;

/**
 * A compression type
 *
 * @author Antoine Willerval
 */
public enum CompressionType {

	/**
	 * gzip compression (.gz .tgz)
	 */
	GZIP("gz", "tgz"),
	/**
	 * bzip compression (.bz2 .bz)
	 */
	BZIP("bz2", "bz"),
	/**
	 * bzip compression (.xz)
	 */
	XZ("xz"),
	/**
	 * no compression
	 */
	NONE;

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

	private final String[] ext;

	CompressionType(String... ext) {
		this.ext = ext;
	}
}

package com.the_qa_company.qendpoint.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Utility class to manage files
 *
 * @author Antoine Willerval
 */
public class FileUtils {
	/**
	 * delete a path recursively
	 *
	 * @param path the path to delete
	 * @throws IOException in case of error
	 */
	public static void deleteRecursively(Path path) throws IOException {
		Files.walkFileTree(path, new FileVisitor<>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) {
				return FileVisitResult.TERMINATE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * open a jar stream or create an exception
	 *
	 * @param filename the filename in the jar
	 * @return stream
	 * @throws IOException if the resource can't be opened
	 */
	public static InputStream openResourceStreamOrIoe(String filename) throws IOException {
		InputStream stream = FileUtils.class.getClassLoader().getResourceAsStream(filename);
		if (stream == null) {
			throw new IOException("Can't open resource file '" + filename + "'");
		}
		return stream;
	}

	/**
	 * open a file in the main directory, then in the current directory and then
	 * the jar, will stop at the first non IOException
	 *
	 * @param main     the main directory
	 * @param filename the filename to open
	 * @return stream
	 * @throws IOException if the stream can't be opened
	 */
	public static InputStream openFile(Path main, String filename) throws IOException {
		IOException exc;

		// open in the main directory
		try {
			return Files.newInputStream(main.resolve(filename));
		} catch (IOException e) {
			exc = e;
		}

		// open in the current directory
		try {
			return new FileInputStream(filename);
		} catch (IOException e) {
			exc.addSuppressed(e);
		}

		// open in the jar
		try {
			return openResourceStreamOrIoe(filename);
		} catch (IOException e) {
			exc.addSuppressed(e);
		}

		throw exc;
	}

	private FileUtils() {
	}
}

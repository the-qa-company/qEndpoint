package com.the_qa_company.qendpoint.utils;

import com.the_qa_company.qendpoint.core.options.ControlInfo;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import org.eclipse.rdf4j.query.resultio.QueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Utility class to get response format
 *
 * @author Antoine Willerval
 */
public class FormatUtils {
	private static class ValuedMime {
		float value;
		String mime;

		public ValuedMime(float value, String mime) {
			this.value = value;
			this.mime = mime;
		}
	}

	private static Stream<ValuedMime> sortedFormat(String acceptHeader) {

		List<ValuedMime> mimes = new ArrayList<>();

		for (String acceptedType : acceptHeader.split("[,]\\s?")) {
			String[] split = acceptedType.split(";");

			String mime = split[0];
			float value;
			if (split.length != 1 && split[1].startsWith("q=")) {
				value = Float.parseFloat(split[1].substring(2));
			} else {
				value = 1;
			}
			mimes.add(new ValuedMime(value, mime));
		}
		return mimes.stream().sorted(Comparator.<ValuedMime>comparingDouble(m -> m.value).reversed());
	}

	/**
	 * get a result writer format for an accept header
	 *
	 * @param acceptHeader the format to search
	 * @return the format
	 */
	public static Optional<QueryResultFormat> getResultWriterFormat(String acceptHeader) {
		return sortedFormat(acceptHeader)
				.map(m -> TupleQueryResultWriterRegistry.getInstance().getFileFormatForMIMEType(m.mime))
				.filter(Optional::isPresent).map(Optional::get).findFirst();
	}

	/**
	 * get a result writer format for an accept header
	 *
	 * @param acceptHeader the format to search
	 * @return the format
	 */
	public static Optional<RDFFormat> getRDFWriterFormat(String acceptHeader) {
		return sortedFormat(acceptHeader).map(m -> Rio.getWriterFormatForMIMEType(m.mime)).filter(Optional::isPresent)
				.map(Optional::get).findFirst();
	}

	/**
	 * Read n bytes from the start of a file
	 *
	 * @param file file
	 * @param size n
	 * @return n bytes, or null if error
	 */
	public static byte[] readCookie(Path file, int size) {
		try (InputStream stream = Files.newInputStream(file)) {
			return (size > 0x1000 ? new BufferedInputStream(stream) : stream).readNBytes(size);
		} catch (IOException e) {
			return new byte[0];
		}
	}

	/**
	 * Read control info from the start of a file
	 *
	 * @param file file
	 * @return ci
	 * @throws IOException ioe
	 */
	public static ControlInfo readCookieInfo(Path file) throws IOException {
		ControlInformation ci = new ControlInformation();
		try (InputStream stream = Files.newInputStream(file)) {
			ci.load(stream);
		}
		return ci;
	}
}

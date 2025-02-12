package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.storage.TempBuffIn;
import com.the_qa_company.qendpoint.core.storage.TempBuffOut;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * tool to profile time
 *
 * @author Antoine Willerval
 */
public class Profiler implements AutoCloseable {
	private static final AtomicLong PROFILER_IDS = new AtomicLong();
	private static final Map<Long, Profiler> PROFILER = new HashMap<>();

	/**
	 * get a non-closed profiler
	 *
	 * @param id profiler id
	 * @return profiler or null if closed or non-existing
	 */
	public static Profiler getProfilerById(long id) {
		return PROFILER.get(id);
	}

	/**
	 * Read the profiling values from an input path
	 *
	 * @param inputPath input path
	 * @throws java.io.IOException                reading exception
	 * @throws java.lang.IllegalArgumentException if the file's CRC doesn't
	 *                                            match
	 */
	public static Profiler readFromDisk(Path inputPath) throws IOException {
		Profiler p = new Profiler("");
		try (InputStream is = new TempBuffIn(Files.newInputStream(inputPath))) {
			for (byte b : HEADER) {
				if (is.read() != b) {
					throw new IOException("Missing header for the profiling file!");
				}
			}
			p.mainSection = p.new Section(is, 0);
			int checkSum = p.mainSection.computeCheckSum();
			int checkSumRead = (int) readLong(is);
			if (checkSumRead != checkSum) {
				throw new IOException("the Checksum isn't the same");
			}
		}
		return p;
	}

	private static long readLong(InputStream is) throws IOException {
		byte[] longBuffer = readBuffer(is, 8);
		return (longBuffer[0] & 0xFF) | ((longBuffer[1] & 0xFFL) << 8) | ((longBuffer[2] & 0xFFL) << 16)
				| ((longBuffer[3] & 0xFFL) << 24) | ((longBuffer[4] & 0xFFL) << 32) | ((longBuffer[5] & 0xFFL) << 40)
				| ((longBuffer[6] & 0xFFL) << 48) | ((longBuffer[7] & 0xFFL) << 56);
	}

	private static void writeLong(OutputStream os, long value) throws IOException {
		os.write((byte) (value & 0xFF));
		os.write((byte) ((value >>> 8) & 0xFF));
		os.write((byte) ((value >>> 16) & 0xFF));
		os.write((byte) ((value >>> 24) & 0xFF));
		os.write((byte) ((value >>> 32) & 0xFF));
		os.write((byte) ((value >>> 40) & 0xFF));
		os.write((byte) ((value >>> 48) & 0xFF));
		os.write((byte) ((value >>> 56) & 0xFF));
	}

	private static byte[] readBuffer(InputStream input, int length) throws IOException {
		int nRead;
		int pos = 0;
		byte[] data = new byte[length];

		while ((nRead = input.read(data, pos, length - pos)) > 0) {
			pos += nRead;
		}

		if (pos != length) {
			throw new EOFException("EOF while reading array from InputStream");
		}

		return data;
	}

	/**
	 * create or load a profiler from the options into a subsection
	 *
	 * @param name    name
	 * @param options options
	 * @param setId   set the id after loading (if required)
	 * @return profiler
	 */
	public static Profiler createOrLoadSubSection(String name, HDTOptions options, boolean setId) {
		return createOrLoadSubSection(name, options, setId, false);
	}

	/**
	 * create or load a profiler from the options into a subsection
	 *
	 * @param name    name
	 * @param options options
	 * @param setId   set the id after loading (if required)
	 * @param async   use async profiler
	 * @return profiler
	 */
	public static Profiler createOrLoadSubSection(String name, HDTOptions options, boolean setId, boolean async) {
		// no options, we can't create
		if (options == null) {
			return new Profiler(name, null, async);
		}
		String profiler = options.get(async ? HDTOptionsKeys.PROFILER_ASYNC_KEY : HDTOptionsKeys.PROFILER_KEY);
		if (profiler != null && profiler.length() != 0 && profiler.charAt(0) == '!') {
			Profiler prof = getProfilerById(Long.parseLong(profiler.substring(1)));
			if (prof != null) {
				prof.pushSection(name);
				prof.deep++;
				return prof;
			}
		}
		// no id, not an id
		Profiler prof = new Profiler(name, options, async);
		if (setId) {
			options.set(async ? HDTOptionsKeys.PROFILER_ASYNC_KEY : HDTOptionsKeys.PROFILER_KEY, prof);
		}
		return prof;
	}

	private static final byte[] HEADER = { 'H', 'D', 'T', 'P', 'R', 'O', 'F', 'I', 'L', 'E' };
	private int maxSize = 0;
	private final String name;
	private Section mainSection;
	private boolean disabled;
	private Path outputPath;
	private final long id;
	private int deep = 0;

	/**
	 * create a disabled profiler
	 *
	 * @param name the profiler name
	 */
	public Profiler(String name) {
		this(name, false);
	}

	/**
	 * create a disabled profiler
	 *
	 * @param name  the profiler name
	 * @param async async profiler
	 */
	public Profiler(String name, boolean async) {
		this(name, null, async);
	}

	/**
	 * create a profiler from specifications
	 *
	 * @param name profiler name
	 * @param spec spec (nullable)
	 */
	public Profiler(String name, HDTOptions spec) {
		this(name, spec, false);
	}

	/**
	 * create a profiler from specifications
	 *
	 * @param name  profiler name
	 * @param spec  spec (nullable)
	 * @param async async profiler
	 */
	public Profiler(String name, HDTOptions spec, boolean async) {
		this.id = PROFILER_IDS.incrementAndGet();
		PROFILER.put(this.id, this);
		this.name = Objects.requireNonNull(name, "name can't be null!");
		if (spec != null) {
			String b = spec.get(async ? HDTOptionsKeys.PROFILER_ASYNC_KEY : HDTOptionsKeys.PROFILER_KEY);
			disabled = b == null || b.length() == 0 || !(b.charAt(0) == '!' || "true".equalsIgnoreCase(b));
			String profilerOutputLocation = spec
					.get(async ? HDTOptionsKeys.PROFILER_ASYNC_OUTPUT_KEY : HDTOptionsKeys.PROFILER_OUTPUT_KEY);
			if (profilerOutputLocation != null && !profilerOutputLocation.isEmpty()) {
				outputPath = Path.of(profilerOutputLocation);
			}
		} else {
			// no profiling by default
			disabled = true;
		}
	}

	/**
	 * disable the profiler methods
	 *
	 * @param disable if true, the methods will be callable, but won't do
	 *                anything
	 */
	public void setDisabled(boolean disable) {
		this.disabled = disable;
	}

	/**
	 * start a section
	 *
	 * @param name the section name
	 */
	public void pushSection(String name) {
		if (disabled) {
			return;
		}
		getMainSection().pushSection(name, 0);
	}

	/**
	 * @return profiler id
	 */
	public long getId() {
		return id;
	}

	public boolean isDisabled() {
		return disabled;
	}

	/**
	 * complete a section
	 */
	public void popSection() {
		if (disabled) {
			return;
		}
		if (!getMainSection().isRunning()) {
			throw new IllegalArgumentException("profiler not running!");
		}
		getMainSection().popSection();
	}

	/**
	 * stop the profiler without poping sections
	 */
	public void stop() {
		if (disabled || deep != 0) {
			return;
		}
		getMainSection().stop();
	}

	/**
	 * reset the profiler
	 */
	public void reset() {
		mainSection = null;
	}

	/**
	 * write the profile into the console
	 */
	public void writeProfiling() throws IOException {
		if (disabled || deep != 0) {
			return;
		}
		getMainSection().writeProfiling("", true);
		if (outputPath != null) {
			writeToDisk(outputPath);
		}
	}

	/**
	 * Write the profiling values into the output path
	 *
	 * @param outputPath output path
	 */
	public void writeToDisk(Path outputPath) throws IOException {
		try (OutputStream os = new TempBuffOut(Files.newOutputStream(outputPath))) {
			for (byte b : HEADER) {
				os.write(b);
			}
			Section mainSection = getMainSection();
			mainSection.writeSection(os);
			writeLong(os, mainSection.computeCheckSum());
		}
	}

	/**
	 * @return the main section of the profiler
	 */
	public Section getMainSection() {
		if (this.mainSection == null) {
			this.mainSection = new Section(name);
			maxSize = Math.max(name.length() + deep * 2, maxSize);
		}
		return this.mainSection;
	}

	@Override
	public void close() {
		if (deep == 0) {
			PROFILER.remove(getId());
		} else {
			deep--;
			popSection();
		}
	}

	/**
	 * a section in the profiling
	 */
	public class Section {
		private final String name;
		private final long start;
		private long end;
		private final List<Section> subSections;
		private transient Section currentSection;

		Section(String name) {
			this.name = name;
			start = System.currentTimeMillis();
			end = start;
			subSections = new ArrayList<>();
		}

		/**
		 * read the section from the input stream
		 *
		 * @param is input stream
		 * @throws IOException io exception
		 */
		Section(InputStream is, int deep) throws IOException {
			start = readLong(is);
			end = readLong(is);

			int nameLength = (int) readLong(is);
			byte[] nameBytes = readBuffer(is, nameLength);
			name = new String(nameBytes, StandardCharsets.UTF_8);

			maxSize = Math.max(name.length() + deep * 2, maxSize);

			int subSize = (int) readLong(is);
			subSections = new ArrayList<>(subSize);
			for (int i = 0; i < subSize; i++) {
				subSections.add(new Section(is, deep + 1));
			}
		}

		void writeSection(OutputStream os) throws IOException {
			byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);

			writeLong(os, start);
			writeLong(os, end);

			writeLong(os, nameBytes.length);
			os.write(nameBytes);

			List<Section> sub = getSubSections();
			writeLong(os, sub.size());

			for (Section s : sub) {
				s.writeSection(os);
			}
		}

		/**
		 * @return the subsections
		 */
		public List<Section> getSubSections() {
			return subSections;
		}

		/**
		 * @return the section name
		 */
		public String getName() {
			return name;
		}

		boolean isRunning() {
			return currentSection != null;
		}

		void pushSection(String name, int deep) {
			if (isRunning()) {
				currentSection.pushSection(name, deep + 1);
				return;
			}

			subSections.add(currentSection = new Section(name));
			maxSize = Math.max(name.length() + deep * 2, maxSize);
		}

		boolean popSection() {
			if (isRunning()) {
				if (currentSection.popSection()) {
					currentSection = null;
				}
				return false;
			} else {
				end = System.currentTimeMillis();
				return true;
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Section section = (Section) o;

			return start == section.start && end == section.end && name.equals(section.name)
					&& subSections.equals(section.subSections);
		}

		@Override
		public int hashCode() {
			int result = name.hashCode();
			result = 31 * result + (int) (start ^ (start >>> 32));
			result = 31 * result + (int) (end ^ (end >>> 32));
			result = 31 * result + subSections.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Section{" + "name='" + name + '\'' + ", start=" + start + ", end=" + end + ", subSections="
					+ subSections + ", currentSection=" + currentSection + '}';
		}

		void stop() {
			if (isRunning()) {
				currentSection.stop();
			}
			end = System.currentTimeMillis();
		}

		public long getMillis() {
			return end - start;
		}

		/**
		 * @return start timestamp
		 */
		public long getStartMillis() {
			return start;
		}

		/**
		 * @return end timestamp
		 */
		public long getEndMillis() {
			return end;
		}

		void writeProfiling(String prefix, boolean isLast) {
			System.out.println(prefix + (getSubSections().isEmpty() ? "+--" : "+-+") + " [" + getName() + "] "
					+ "-".repeat(1 + maxSize - getName().length()) + " elapsed=" + getMillis() + "ms");
			for (int i = 0; i < subSections.size(); i++) {
				Section s = subSections.get(i);
				s.writeProfiling(prefix + (isLast ? "  " : "| "), i == subSections.size() - 1);
			}
		}

		/**
		 * @return checksum for the profiling section
		 */
		public int computeCheckSum() {
			int result = name.length();
			result = 31 * result + (int) (start ^ (start >>> 32));
			result = 31 * result + (int) (end ^ (end >>> 32));
			for (Section subSection : subSections) {
				result = 31 * result ^ subSection.computeCheckSum();
			}
			return result;
		}
	}
}

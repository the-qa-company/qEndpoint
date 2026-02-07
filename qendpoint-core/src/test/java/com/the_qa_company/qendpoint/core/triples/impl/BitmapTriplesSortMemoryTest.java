package com.the_qa_company.qendpoint.core.triples.impl;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

public class BitmapTriplesSortMemoryTest {
	@Test(timeout = 120_000)
	public void sortByValueThenPositionSurvivesLowHeap() throws Exception {
		runChildJvm(SortMain.class, "64m", 110, TimeUnit.SECONDS);
	}

	@Test(timeout = 120_000)
	public void sortByValueThenPositionSurvivesConcurrentLowHeap() throws Exception {
		runChildJvm(SortConcurrentMain.class, "256m", 110, TimeUnit.SECONDS);
	}

	@Test
	public void timSortTempBufferDoesNotExceedHalf() throws Exception {
		boolean poolEnabled = readLongArrayPoolEnabled();
		LongArrayPool.setEnabled(false);
		try {
			int length = 1024;
			long[] values = new long[length];
			long[] positions = new long[length];

			Class<?> timSortClass = Class
					.forName("com.the_qa_company.qendpoint.core.triples.impl.BitmapTriples$LongPairTimSort");
			Constructor<?> ctor = timSortClass.getDeclaredConstructor(long[].class, long[].class, int.class);
			ctor.setAccessible(true);
			Object sorter = ctor.newInstance(values, positions, length);

			Method ensureCapacity = timSortClass.getDeclaredMethod("ensureCapacity", int.class);
			ensureCapacity.setAccessible(true);
			ensureCapacity.invoke(sorter, length / 2);

			Field tmpValuesField = timSortClass.getDeclaredField("tmpValues");
			tmpValuesField.setAccessible(true);
			long[] tmpValues = (long[]) tmpValuesField.get(sorter);
			if (tmpValues.length > length / 2) {
				fail("Temp buffer grew to " + tmpValues.length + " for length " + length);
			}
		} finally {
			LongArrayPool.setEnabled(poolEnabled);
		}
	}

	private static List<String> buildCommand(Class<?> mainClass, String maxHeap) {
		String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
		String classpath = System.getProperty("java.class.path");
		List<String> command = new ArrayList<>();
		command.add(javaBin);
		command.add("-Xmx" + maxHeap);
		command.add("-XX:+ExitOnOutOfMemoryError");
		command.add("-cp");
		command.add(classpath);
		command.add(mainClass.getName());
		return command;
	}

	private static boolean readLongArrayPoolEnabled() throws Exception {
		Field enabledField = LongArrayPool.class.getDeclaredField("enabled");
		enabledField.setAccessible(true);
		return (boolean) enabledField.get(null);
	}

	private static void runChildJvm(Class<?> mainClass, String maxHeap, long timeout, TimeUnit unit) throws Exception {
		ProcessBuilder builder = new ProcessBuilder(buildCommand(mainClass, maxHeap));
		builder.redirectErrorStream(true);
		Process process = builder.start();

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		AtomicReference<Throwable> readFailure = new AtomicReference<>();
		Thread reader = new Thread(() -> {
			try (InputStream stream = process.getInputStream()) {
				stream.transferTo(output);
			} catch (Throwable err) {
				readFailure.compareAndSet(null, err);
			}
		}, "bitmaptriples-sort-memory-reader");
		reader.setDaemon(true);
		reader.start();

		boolean finished = process.waitFor(timeout, unit);
		if (!finished) {
			process.destroyForcibly();
			process.waitFor(10, TimeUnit.SECONDS);
		}
		reader.join(unit.toMillis(5));

		Throwable readError = readFailure.get();
		if (readError != null) {
			throw new RuntimeException("Failed to read child JVM output", readError);
		}

		String outputText = output.toString(StandardCharsets.UTF_8);
		if (!finished) {
			fail("Child JVM timed out after " + timeout + " " + unit.toString().toLowerCase() + "\n" + outputText);
		}
		int exitCode = process.exitValue();
		if (exitCode != 0) {
			fail("Child JVM failed with exit code " + exitCode + "\n" + outputText);
		}
	}

	private static int resolveLength(int threads) {
		long maxHeap = Runtime.getRuntime().maxMemory();
		long usable = (long) (maxHeap * 0.35);
		long perElement = 16L * threads;
		long length = usable / perElement;
		if (length < 250_000L) {
			length = 250_000L;
		}
		if (length > 3_000_000L) {
			length = 3_000_000L;
		}
		return (int) length;
	}

	public static final class SortMain {
		public static void main(String[] args) throws Exception {
			int length = resolveLength(1);
			long[] values = new long[length];
			long[] positions = new long[length];
			long seed = 1L;
			for (int i = 0; i < length; i++) {
				seed = seed * 6364136223846793005L + 1;
				values[i] = seed;
				positions[i] = i;
			}
			Method sortMethod = BitmapTriples.class.getDeclaredMethod("sortByValueThenPosition", long[].class,
					long[].class, int.class, int.class);
			sortMethod.setAccessible(true);
			sortMethod.invoke(null, values, positions, 0, length);
		}
	}

	public static final class SortConcurrentMain {
		public static void main(String[] args) throws Exception {
			int threads = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors()));
			int length = resolveLength(threads);
			Method sortMethod = BitmapTriples.class.getDeclaredMethod("sortByValueThenPosition", long[].class,
					long[].class, int.class, int.class);
			sortMethod.setAccessible(true);

			CountDownLatch ready = new CountDownLatch(threads);
			CountDownLatch start = new CountDownLatch(1);
			CountDownLatch done = new CountDownLatch(threads);
			AtomicReference<Throwable> failure = new AtomicReference<>();

			for (int t = 0; t < threads; t++) {
				long[] values = new long[length];
				long[] positions = new long[length];
				long seed = t + 1L;
				for (int i = 0; i < length; i++) {
					seed = seed * 6364136223846793005L + 1;
					values[i] = seed;
					positions[i] = i;
				}
				long[] threadValues = values;
				long[] threadPositions = positions;
				Thread thread = new Thread(() -> {
					ready.countDown();
					try {
						start.await();
						sortMethod.invoke(null, threadValues, threadPositions, 0, length);
					} catch (Throwable err) {
						failure.compareAndSet(null, err);
					} finally {
						done.countDown();
					}
				}, "sort-" + t);
				thread.start();
			}

			ready.await();
			start.countDown();
			done.await();

			Throwable thrown = failure.get();
			if (thrown != null) {
				throw new RuntimeException(thrown);
			}
		}
	}
}

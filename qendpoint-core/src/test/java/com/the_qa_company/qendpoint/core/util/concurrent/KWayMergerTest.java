package com.the_qa_company.qendpoint.core.util.concurrent;

import com.the_qa_company.qendpoint.core.compact.integer.VByte;
import com.the_qa_company.qendpoint.core.iterator.utils.AsyncIteratorFetcher;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.MergeExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.SizeFetcher;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class KWayMergerTest {
	@Parameterized.Parameters(name = "{0}-worker-{1}-ways-({2}-{3})")
	public static Collection<Object[]> params() {
		final int element = 10_000;
		final int split = 100;
		final int hugeMemoryFactor = 4;
		return List.of(new Object[] { 1, 8, split, element }, new Object[] { 2, 8, split, element },
				new Object[] { 8, 8, split, element }, new Object[] { 1, 2, split, element },
				new Object[] { 2, 2, split, element }, new Object[] { 8, 2, split, element },
				new Object[] { 1, 8, split * hugeMemoryFactor, element },
				new Object[] { 2, 8, split * hugeMemoryFactor, element },
				new Object[] { 8, 8, split * hugeMemoryFactor, element },
				new Object[] { 1, 2, split * hugeMemoryFactor, element },
				new Object[] { 2, 2, split * hugeMemoryFactor, element },
				new Object[] { 8, 2, split * hugeMemoryFactor, element });
	}

	@Parameterized.Parameter
	public int workers;
	@Parameterized.Parameter(1)
	public int k;
	@Parameterized.Parameter(2)
	public int splitSize;
	@Parameterized.Parameter(3)
	public int elements;
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void simpleMerge() throws IOException, KWayMerger.KWayMergerException, InterruptedException {
		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			Random rnd = new Random(64);
			List<Integer> values = IntStream.iterate(2, s -> 1 + rnd.nextInt(elements * 10)).limit(elements).boxed()
					.collect(Collectors.toCollection(ArrayList::new));

			assert values.stream().mapToInt(c -> c).min().orElse(1) > 0;

			List<Integer> expected = new ArrayList<>(values);
			expected.sort(Integer::compareTo);
			// assertNotEquals(values, expected); // uncomment it if you don't
			// trust me...

			KWayMerger<Integer, Supplier<Integer>> merger = new KWayMerger<>(root,
					new AsyncIteratorFetcher<>(values.iterator()), new KWayMerger.KWayMergerImpl<>() {
						@Override
						public void createChunk(Supplier<Integer> flux, CloseSuppressPath output)
								throws KWayMerger.KWayMergerException {
							Integer v;
							List<Integer> obj = new ArrayList<>();
							while ((v = flux.get()) != null) {
								obj.add(v);
							}
							obj.sort(Integer::compareTo);

							try (OutputStream os = output.openOutputStream(1024)) {
								for (Integer i : obj) {
									VByte.encode(os, i);
								}
								VByte.encode(os, 0);
							} catch (IOException e) {
								throw new KWayMerger.KWayMergerException(e);
							}
						}

						@Override
						public void mergeChunks(List<CloseSuppressPath> inputs, CloseSuppressPath output)
								throws KWayMerger.KWayMergerException {
							try {
								List<List<Integer>> lists = new ArrayList<>();
								for (CloseSuppressPath path : inputs) {
									List<Integer> list = new ArrayList<>();
									try (InputStream is = path.openInputStream(1024)) {
										while (true) {
											long value = VByte.decode(is);

											if (value == 0) {
												break;
											}

											list.add((int) value);
										}
									}
									lists.add(list);
								}
								ExceptionIterator<Integer, RuntimeException> merge = MergeExceptionIterator.buildOfTree(
										e -> ExceptionIterator.of(e.iterator()), Integer::compareTo, lists, 0,
										lists.size());

								try (OutputStream os = output.openOutputStream(1024)) {
									while (merge.hasNext()) {
										VByte.encode(os, merge.next());
									}
									VByte.encode(os, 0);
								} catch (IOException e) {
									throw new KWayMerger.KWayMergerException(e);
								}
								IOUtil.closeAll(inputs);
							} catch (IOException e) {
								throw new KWayMerger.KWayMergerException(e);
							}
						}

						@Override
						public Supplier<Integer> newStopFlux(Supplier<Integer> flux) {
							return new SizeFetcher<>(flux, e -> 1, splitSize);
						}
					}, workers, k);

			merger.start();
			Optional<CloseSuppressPath> paths = merger.waitResult();

			assertFalse(paths.isEmpty());
			CloseSuppressPath end = paths.get();

			List<Integer> actual = new ArrayList<>();
			try (InputStream is = end.openInputStream(1024)) {
				while (true) {
					long value = VByte.decode(is);

					if (value == 0) {
						break;
					}

					actual.add((int) value);
				}
			}

			assertEquals(expected, actual);
		}

	}

}

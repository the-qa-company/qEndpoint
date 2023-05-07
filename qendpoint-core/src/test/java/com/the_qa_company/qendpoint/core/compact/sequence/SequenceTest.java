package com.the_qa_company.qendpoint.core.compact.sequence;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

@RunWith(Parameterized.class)
public class SequenceTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object> params() {
		return Arrays.asList(new SequenceGenerator("SequenceLog64BigDisk", SequenceLog64BigDisk::new),
				new SequenceGenerator("SequenceLog64",
						((workFile, bits, elements) -> new SequenceLog64(bits, elements))),
				new SequenceGenerator("SequenceLog64Big",
						((workFile, bits, elements) -> new SequenceLog64Big(bits, elements))));
	}

	@Parameterized.Parameter
	public SequenceGenerator sequenceGenerator;

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	private void sequenceTest(int bits, long elements, boolean trim) throws IOException {
		long maxMask = (~0L) >>> (Long.SIZE - bits);

		Path p = tempDir.newFolder().toPath();
		try (DynamicSequence actual = sequenceGenerator.bld.generate(p.resolve("test.seq").toString(), trim ? 64 : bits,
				elements)) {
			{
				Random rnd = new Random(32);
				for (long i = 0; i < elements; i++) {
					long v = rnd.nextLong() & maxMask;
					if (v < 0) {
						v = -v;
					}
					actual.append(v);
				}
			}
			{
				Random rnd = new Random(32);
				for (long i = 0; i < elements; i++) {
					long v = rnd.nextLong() & maxMask;
					if (v < 0) {
						v = -v;
					}
					Assert.assertEquals(actual.get(i), v);
				}
			}
			if (trim) {
				actual.aggressiveTrimToSize();
			}
			{
				Random rnd = new Random(32);
				for (long i = 0; i < elements; i++) {
					long v = rnd.nextLong() & maxMask;
					if (v < 0) {
						v = -v;
					}
					Assert.assertEquals("actual fail", actual.get(i), v);
				}
			}
		}
	}

	@Test
	public void littleTest() throws IOException {
		sequenceTest(64, 100L, false);
	}

	@Test
	public void bit64Test() throws IOException {
		sequenceTest(64, 10_000L, false);
	}

	@Test
	public void bit32Test() throws IOException {
		sequenceTest(32, 10_000L, false);
	}

	@Test
	public void bit64TrimTest() throws IOException {
		sequenceTest(64, 10_000L, true);
	}

	@Test
	public void bit32TrimTest() throws IOException {
		sequenceTest(32, 10_000L, true);
	}

	private static class SequenceGenerator {
		final String name;
		final SequenceGeneratorBuilder bld;

		public SequenceGenerator(String name, SequenceGeneratorBuilder bld) {
			this.name = name;
			this.bld = bld;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	@FunctionalInterface
	private interface SequenceGeneratorBuilder {
		DynamicSequence generate(String workFile, int bits, long elements);
	}
}

package com.the_qa_company.qendpoint.core.compact.sequence;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

public class SequenceLog64BigDiskSetTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void overwrite64BitValueClearsBits() throws IOException {
		Path path = tempDir.newFile("seq64.bin").toPath();
		try (SequenceLog64BigDisk sequence = new SequenceLog64BigDisk(path, 64, 1, true)) {
			long initial = 1L << 62;
			sequence.set(0, initial);
			sequence.set(0, 0L);
			Assert.assertEquals(0L, sequence.get(0));
		}
	}

	@Test
	public void bulkSetStoresValuesAcrossWords() throws IOException {
		Path path = tempDir.newFile("seq64-bulk.bin").toPath();
		int bits = 13;
		try (SequenceLog64BigDisk sequence = new SequenceLog64BigDisk(path, bits, 16, true)) {
			long[] positions = { 9, 4, 1, 0 };
			long[] values = { 8190L, 5461L, 291L, 3L };
			long[] expectedPositions = positions.clone();
			long[] expectedValues = values.clone();

			sequence.set(positions, values, 0, positions.length);

			for (int i = 0; i < expectedPositions.length; i++) {
				Assert.assertEquals(expectedValues[i], sequence.get(expectedPositions[i]));
			}
		}
	}
}

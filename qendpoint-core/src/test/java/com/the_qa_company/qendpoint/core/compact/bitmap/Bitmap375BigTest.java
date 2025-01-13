package com.the_qa_company.qendpoint.core.compact.bitmap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.Assert.*;

public class Bitmap375BigTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void calcTest() throws IOException {
		Path root = tempDir.newFolder().toPath();

		Path arrPath = root.resolve("arr.bin");
		Path arr2Path = root.resolve("arr2.bin");

		long nbits = 12345;
		long seed = 73;
		try (Bitmap375Big disk = Bitmap375Big.map(arrPath, nbits, true)) {
			{
				// set default data
				Random rnd = new Random(seed);

				for (int i = 0; i < nbits / 40; i++) {
					disk.set(rnd.nextLong(seed), true);
				}
			}
			{
				// test after set
				Random rnd = new Random(seed);
				for (int i = 0; i < nbits / 40; i++) {
					long idx = rnd.nextLong(seed);
					assertTrue("bad arr #" + idx + "/" + i, disk.access(idx));
				}
			}
			disk.updateIndex();
			{
				// test after update index
				Random rnd = new Random(seed);
				for (int i = 0; i < nbits / 40; i++) {
					long idx = rnd.nextLong(seed);
					assertTrue("bad arr #" + idx + "/" + i, disk.access(idx));
				}
			}
			disk.savePath(arr2Path);
		}

		@SuppressWarnings("resource")
		Bitmap375Big mem = Bitmap375Big.memory(0);

		mem.loadPath(arr2Path);
		{
			// test after mapping
			Random rnd = new Random(seed);
			for (int i = 0; i < nbits / 40; i++) {
				long idx = rnd.nextLong(seed);
				assertTrue("bad arr #" + idx + "/" + i, mem.access(idx));
			}
		}
		mem.updateIndex();
		{
			// test after mapping
			Random rnd = new Random(seed);
			for (int i = 0; i < nbits / 40; i++) {
				long idx = rnd.nextLong(seed);
				assertTrue("bad arr #" + idx + "/" + i, mem.access(idx));
			}
		}
	}
}
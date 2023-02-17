package com.the_qa_company.qendpoint.core.compact.bitmap;

import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class AppendableWriteBitmapTest {

	public static void assertFileEquals(Path excepted, Path actual) throws IOException {
		byte[] be = Files.readAllBytes(excepted);
		byte[] ba = Files.readAllBytes(actual);
		Assert.assertEquals("File sizes aren't the same", be.length, ba.length);
		for (int i = 0; i < be.length; i++) {
			Assert.assertEquals("bytes #" + i + " aren't matching", be[i], ba[i]);
		}
	}

	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	@Test
	public void bitmapTest() throws IOException {
		Path dir = tempDir.newFolder().toPath();
		Path bitmapE = dir.resolve("bme.bin");
		Path bitmapA = dir.resolve("bma.bin");

		ModifiableBitmap bitmap = BitmapFactory.createRWBitmap(160);
		AppendableWriteBitmap writeBitmap = new AppendableWriteBitmap(CloseSuppressPath.of(dir.resolve("compute.bin")),
				CloseSuppressPath.BUFFER_SIZE);

		for (int i = 0; i < bitmap.getNumBits(); i++) {
			bitmap.append(i % 2 == 0);
			writeBitmap.append(i % 2 == 0);
		}

		try (OutputStream stream = Files.newOutputStream(bitmapE)) {
			bitmap.save(stream, null);
		}
		try (OutputStream stream = Files.newOutputStream(bitmapA)) {
			writeBitmap.save(stream, null);
		}

		assertFileEquals(bitmapE, bitmapA);
	}
}

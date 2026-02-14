package com.the_qa_company.qendpoint.core.dictionary;

import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.SectionCompressor;
import com.the_qa_company.qendpoint.core.listener.MultiThreadListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class DictionaryFactoryMergeConcurrencyTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void createSectionCompressorPullHonorsConfiguredMergeConcurrencyBelowFour() throws Exception {
		HDTOptions spec = HDTOptions.of();
		spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "8");
		spec.set(HDTOptionsKeys.LOADER_DISK_MERGE_CONCURRENCY_KEY, "3");

		try (CloseSuppressPath baseFileName = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			SectionCompressor compressor = DictionaryFactory.createSectionCompressorPull(spec, baseFileName,
					MultiThreadListener.ignore(), 64, 1L, 2, false, CompressionType.NONE);

			assertEquals(3, readMaxConcurrentMerges(compressor));
		}
	}

	private static int readMaxConcurrentMerges(SectionCompressor compressor) throws Exception {
		Field field = SectionCompressor.class.getDeclaredField("maxConcurrentMerges");
		field.setAccessible(true);
		return field.getInt(compressor);
	}
}

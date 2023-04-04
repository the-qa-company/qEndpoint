package com.the_qa_company.qendpoint.core.storage;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import org.apache.commons.io.file.PathUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class QEPCoreTest {
	@Parameterized.Parameters(name = "test")
	public static Collection<Object[]> params() throws IOException, ParserException {
		Path root = Files.createTempDirectory("qepCoreTest");
		Path[] splitHDT;
		try {

			LargeFakeDataSetStreamSupplier supplier
					= LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(100_000, 34)
					.withMaxElementSplit(200)
					.withMaxLiteralSize(100);

			HDTOptions spec = HDTOptions.of(
					HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS,
					HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,
					HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, root.resolve("gen")
			);

			Path rootHDT = root.resolve("root.hdt");
			try (HDT fakeHDT = supplier.createFakeHDT(spec)) {
				fakeHDT.saveToHDT(rootHDT, ProgressListener.ignore());
			}

			splitHDT = IOUtil.splitHDT(rootHDT, root.resolve("split"), 10);

		} catch (Throwable t) {
			PathUtils.deleteDirectory(root);
			throw t;
		}
		return List.of(new Object[][]{
				{root, splitHDT}
		});
	}

	@Parameterized.AfterParam
	public static void after(Path root, Path[] splitHDT) throws IOException {
		PathUtils.deleteDirectory(root);
	}

	@Parameterized.Parameter
	public Path root;
	@Parameterized.Parameter(1)
	public Path[] splitHDT;

	@Test
	public void a() {
		System.out.println("test 1");
		IOUtil.printPath(root);
	}

}

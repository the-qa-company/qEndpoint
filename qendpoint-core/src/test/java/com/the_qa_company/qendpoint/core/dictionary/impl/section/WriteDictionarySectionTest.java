package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.iterator.utils.MapIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.apache.commons.io.file.PathUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class WriteDictionarySectionTest {
	@Rule
	public TemporaryFolder tempDir = new TemporaryFolder();

	private LargeFakeDataSetStreamSupplier stream() {
		return LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(10_000, 75).withUnicode(true)
				.withMaxLiteralSize(20).withMaxElementSplit(50);
	}

	@Test
	public void appenderTest() throws IOException {
		Path dir = tempDir.getRoot().toPath();
		try {

			try (WriteDictionarySection section1 = new WriteDictionarySection(HDTOptions.of(), dir.resolve("t1"), 4096);
					WriteDictionarySection section2 = new WriteDictionarySection(HDTOptions.of(), dir.resolve("t2"),
							4096)) {
				Iterator<? extends CharSequence> it1 = stream().objectIterator();
				Iterator<? extends CharSequence> it2 = stream().objectIterator();

				section1.load(new MapIterator<>(it1, ByteString::of), 10_000, ProgressListener.ignore());

				try (WriteDictionarySection.WriteDictionarySectionAppender appender = section2.createAppender(10_000,
						ProgressListener.ignore())) {
					while (it2.hasNext()) {
						CharSequence next = it2.next();
						appender.append(ByteString.of(next));
					}
				}
				Path t1Save = dir.resolve("t1.save");
				Path t2Save = dir.resolve("t2.save");
				try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(t1Save))) {
					section1.save(os, ProgressListener.ignore());
				}
				try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(t2Save))) {
					section2.save(os, ProgressListener.ignore());
				}

				byte[] t1Bytes = Files.readAllBytes(t1Save);
				byte[] t2Bytes = Files.readAllBytes(t2Save);

				Assert.assertArrayEquals(t1Bytes, t2Bytes);

			}
		} finally {
			PathUtils.deleteDirectory(dir);
		}
	}
}

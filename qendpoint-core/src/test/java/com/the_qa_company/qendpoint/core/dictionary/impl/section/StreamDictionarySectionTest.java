package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.LargeFakeDataSetStreamSupplier;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IntegrityObject;
import com.the_qa_company.qendpoint.core.util.string.CharSequenceComparator;
import com.the_qa_company.qendpoint.core.util.string.CompactString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class StreamDictionarySectionTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void mapTest() throws IOException {
		Path root = tempDir.newFolder().toPath();

		HDTOptions spec = HDTOptions.of();
		Path res = root.resolve("res.bin");

		LargeFakeDataSetStreamSupplier su = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(10000, 42);
		List<CharSequence> list = new ArrayList<>();
		su.createObjectsStream().forEachRemaining(s -> list.add(new CompactString(s)));

		Comparator<CharSequence> comp = CharSequenceComparator.getInstance();
		list.sort(comp);

		try (WriteStreamDictionarySection write = new WriteStreamDictionarySection(spec, res, 4096)) {
			write.load(list.iterator(), list.size(), ProgressListener.ignore());
			write.save(res);
		}

		try (CountInputStream cis = new CountInputStream(new BufferedInputStream(Files.newInputStream(res)))) {
			try (DictionarySectionPrivate sec = DictionarySectionFactory.loadFrom(cis, res.toFile(),
					ProgressListener.ignore())) {

				IntegrityObject.checkObjectIntegrity(ProgressListener.ignore(), sec);

				Iterator<? extends CharSequence> it = sec.getSortedEntries();

				int idx = 0;
				for (; it.hasNext(); idx++) {
					CharSequence bs = it.next();
					CharSequence ex = list.get(idx);

					if (comp.compare(bs, ex) != 0) {
						fail("bad string for index #" + idx + " \n" + "Expected: " + ex + "\n" + "Actual  : " + bs
								+ "\n");
					}
				}
				assertEquals(idx, list.size());
			}

		}
	}

	@Test
	public void loadTest() throws IOException {
		Path root = tempDir.newFolder().toPath();

		HDTOptions spec = HDTOptions.of();
		Path res = root.resolve("res.bin");

		LargeFakeDataSetStreamSupplier su = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(10000, 42);
		List<CharSequence> list = new ArrayList<>();
		su.createObjectsStream().forEachRemaining(s -> list.add(new CompactString(s)));

		Comparator<CharSequence> comp = CharSequenceComparator.getInstance();
		list.sort(comp);

		try (WriteStreamDictionarySection write = new WriteStreamDictionarySection(spec, res, 4096)) {
			write.load(list.iterator(), list.size(), ProgressListener.ignore());
			write.save(res);
		}

		try (CountInputStream cis = new CountInputStream(new BufferedInputStream(Files.newInputStream(res)))) {
			DictionarySectionPrivate sec = DictionarySectionFactory.loadFrom(cis, ProgressListener.ignore());

			Iterator<? extends CharSequence> it = sec.getSortedEntries();

			int idx = 0;
			for (; it.hasNext(); idx++) {
				CharSequence bs = it.next();
				CharSequence ex = list.get(idx);

				if (comp.compare(bs, ex) != 0) {
					fail("bad string for index #" + idx + " \n" + "Expected: " + ex + "\n" + "Actual  : " + bs + "\n");
				}
			}

			assertEquals(idx, list.size());

		}
	}

	@Test
	public void indexTest() throws IOException, ParserException {
		HDTOptions spec = HDTOptions.of();
		Path root = tempDir.newFolder().toPath();
		Files.createDirectories(root);
		Path genPath = root.resolve("gen.hdt");
		Path genexPath = root.resolve("genex.hdt");
		HDTManager.setupDiskOptions(spec, genPath, root.resolve("work"));

		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(10000, 42);

		supplier.reset();
		supplier.createAndSaveFakeHDT(spec, genexPath);

		supplier.reset();
		spec.setOptions(
				HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK,
				HDTOptionsKeys.DISK_WRITE_SECTION_TYPE_KEY, HDTOptionsKeys.DISK_WRITE_SECTION_TYPE_VALUE_STREAM
		);
		supplier.createAndSaveFakeHDT(spec, genPath);

		try (
				HDT ac = HDTManager.mapHDT(genPath);
				HDT ex = HDTManager.mapHDT(genexPath);
				) {
			IntegrityObject.checkAllIntegrity(ProgressListener.ignore(), ex, ac);
		}

	}
}

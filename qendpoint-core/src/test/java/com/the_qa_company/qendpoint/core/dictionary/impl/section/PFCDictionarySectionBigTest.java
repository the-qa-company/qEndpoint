package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.iterator.utils.EmptyIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import org.apache.commons.io.file.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PFCDictionarySectionBigTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void loadSaveEmptyTest() throws IOException {
		Path root = tempDir.newFolder().toPath();

		try {
			Path secPath = root.resolve("dicsec.bin");
			try (PFCDictionarySectionBig sec = new PFCDictionarySectionBig(HDTOptions.empty())) {
				sec.load(new EmptyIterator<>(), 0, ProgressListener.ignore());
				sec.save(secPath);
			}

			try (PFCDictionarySectionBig sec = new PFCDictionarySectionBig(HDTOptions.empty());
					InputStream is = new BufferedInputStream(Files.newInputStream(secPath))) {
				sec.load(is, ProgressListener.ignore());
				sec.save(secPath);

				Iterator<CharSequence> it = sec.getSortedEntries();
				assertFalse(it.hasNext());
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}
	}

	@Test
	public void loadSave1ElTest() throws IOException {
		Path root = tempDir.newFolder().toPath();

		Set<String> set = Set.of("aaaa", "bbbb", "cccc"

		);

		try {
			Path secPath = root.resolve("dicsec.bin");
			try (PFCDictionarySectionBig sec = new PFCDictionarySectionBig(HDTOptions.empty())) {
				sec.load(set.iterator(), 0, ProgressListener.ignore());
				sec.save(secPath);
			}

			try (PFCDictionarySectionBig sec = new PFCDictionarySectionBig(HDTOptions.empty());
					InputStream is = new BufferedInputStream(Files.newInputStream(secPath))) {
				sec.load(is, ProgressListener.ignore());
				sec.save(secPath);

				Iterator<CharSequence> it = sec.getSortedEntries();
				for (int i = 0; i < set.size(); i++) {
					assertTrue(it.hasNext());
					String string = it.next().toString();
					assertTrue("missing:" + string, set.contains(string));
				}
				assertFalse(it.hasNext());
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}
	}

}

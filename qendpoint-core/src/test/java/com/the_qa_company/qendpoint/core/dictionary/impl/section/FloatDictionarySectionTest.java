package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class FloatDictionarySectionTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();
	@Test
	public void loadTest() throws IOException {
		Random rnd = new Random(456789);
		// iterations
		for (int z = 0; z < 4; z++) {
			// 50 to avoid reaching long limit
			for (int i = 2; i < 50; i++) {
				FloatDictionarySection sec = new FloatDictionarySection(HDTOptions.empty());

				int count = rnd.nextInt(0x2000); // 2^14

				long bound = 1L << i;

				double curr = bound * (rnd.nextDouble() - 0.5);
				List<ByteString> strings = new ArrayList<>(count);
				strings.add(ByteString.of(curr));
				for (int j = 0; j < count; j++) {
					double vl = rnd.nextDouble() * bound;
					double prev = curr;
					curr += vl;
					if (Double.doubleToLongBits(curr) == Double.doubleToLongBits(prev)) continue;

					strings.add(ByteString.of(curr));
				}

				sec.load(
						strings.iterator(),
						strings.size(),
						ProgressListener.ignore()
				);

				{ // test str
					Iterator<? extends CharSequence> it = sec.getSortedEntries();
					Iterator<ByteString> itex = strings.iterator();
					long id = 0;
					while (it.hasNext()) {
						CharSequence its = it.next();
						id++;
						assertTrue("too many elements: " + id, itex.hasNext());
						ByteString itsex = itex.next();
						assertEquals("bad elem: " + id, itsex, its);
						assertEquals(itsex, sec.extract(id));
					}
					assertFalse("not enough elements " + id, itex.hasNext());
				}

				{
					Iterator<? extends CharSequence> it = sec.getSortedEntries();
					long id = 0;
					while (it.hasNext()) {
						id++;
						CharSequence bs = it.next();
						assertEquals("bad idx for " + bs, id, sec.locate(bs));
					}
				}

				{
					long id = 0;
					for (ByteString itsex : strings) {
						id++;
						assertEquals("bad idx for " + itsex, id, sec.locate(itsex));
					}
				}


				sec.close();
			}

		}
	}

	@Test
	public void saveTest() throws IOException {
		Path root = tempDir.newFolder().toPath();
		Random rnd = new Random(456789);
		// iterations
		for (int z = 0; z < 4; z++) {
			// 50 to avoid reaching long limit
			for (int i = 2; i < 50; i++) {
				try (
						DictionarySectionPrivate sec = new FloatDictionarySection(HDTOptions.empty());
						DictionarySectionPrivate sec2 = new FloatDictionarySection(HDTOptions.empty())
				){
					int count = rnd.nextInt(0x2000); // 2^14

					long bound = 1L << i;

					double curr = bound * (rnd.nextDouble() - 0.5);
					List<ByteString> strings = new ArrayList<>(count);
					strings.add(ByteString.of(curr));
					for (int j = 0; j < count; j++) {
						double vl = rnd.nextDouble() * bound;
						double prev = curr;
						curr += vl;
						if (Double.doubleToLongBits(curr) == Double.doubleToLongBits(prev)) continue;

						strings.add(ByteString.of(curr));
					}

					sec.load(
							strings.iterator(),
							strings.size(),
							ProgressListener.ignore()
					);

					Path idx = root.resolve("idx.bin");
					sec.save(idx, ProgressListener.ignore());

					sec2.load(idx, ProgressListener.ignore());

					Iterator<? extends CharSequence> itex = sec.getSortedEntries();
					Iterator<? extends CharSequence> itac = sec2.getSortedEntries();

					assertEquals("bad size", strings.size(), sec.getNumberOfElements());
					assertEquals("not the same size", sec.getNumberOfElements(), sec2.getNumberOfElements());

					while (itex.hasNext()) {
						assertTrue("not enough elements", itac.hasNext());
						CharSequence excepted = itex.next();
						CharSequence actual = itac.next();

						assertEquals("invalid element", excepted, actual);
					}
					assertFalse("too many elements", itac.hasNext());
				} catch (Throwable t) {
					try {
						IOUtil.deleteDirRecurse(root);
					} catch (IOException e) {
						t.addSuppressed(e);
					}
				}
				IOUtil.deleteDirRecurse(root);
			}

		}
	}

}
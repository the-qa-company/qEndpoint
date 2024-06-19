package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class DecimalDictionarySectionTest {

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void loadTest() throws IOException {
		Random rnd = new Random(456789);
		// iterations
		for (int z = 0; z < 4; z++) {
			// 50 to avoid reaching long limit
			for (int i = 2; i < 50; i++) {
				DecimalDictionarySection sec = new DecimalDictionarySection(HDTOptions.empty());

				int count = rnd.nextInt(0x2000); // 2^14

				long bound = 1L << i;

				double curr = bound * (rnd.nextDouble() - 0.5);
				List<ByteString> strings = new ArrayList<>(count);
				strings.add(ByteString.of(BigDecimal.valueOf(curr)));
				for (int j = 0; j < count; j++) {
					double vl = rnd.nextDouble() * bound;
					double prev = curr;
					curr += vl;
					if (Double.doubleToLongBits(curr) == Double.doubleToLongBits(prev))
						continue;

					strings.add(ByteString.of(BigDecimal.valueOf(curr)));
				}

				sec.load(strings.iterator(), strings.size(), ProgressListener.ignore());

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
				try (DictionarySectionPrivate sec = new DecimalDictionarySection(HDTOptions.empty());
						DictionarySectionPrivate sec2 = new DecimalDictionarySection(HDTOptions.empty())) {

					int count = rnd.nextInt(0x2000); // 2^14

					long bound = 1L << i;

					double curr = bound * (rnd.nextDouble() - 0.5);
					List<ByteString> strings = new ArrayList<>(count);
					strings.add(ByteString.of(BigDecimal.valueOf(curr)));
					for (int j = 0; j < count; j++) {
						double vl = rnd.nextDouble() * bound;
						double prev = curr;
						curr += vl;
						if (Double.doubleToLongBits(curr) == Double.doubleToLongBits(prev))
							continue;

						strings.add(ByteString.of(BigDecimal.valueOf(curr)));
					}
					sec.load(strings.iterator(), strings.size(), ProgressListener.ignore());

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
					throw t;
				}
			}

		}
		IOUtil.deleteDirRecurse(root);
	}

	@Test
	@Ignore("test")
	public void saveLenDeltaTest() throws IOException {
		Path root = tempDir.newFolder().toPath();
		Random rnd = new Random(456789);
		// iterations
		for (int z = 0; z < 4; z++) {
			// 50 to avoid reaching long limit
			for (int i = 2; i < 50; i++) {
				try (DictionarySectionPrivate sec = new DecimalDictionarySection(HDTOptions.empty());
						DictionarySectionPrivate secEx = new PFCDictionarySectionBig(HDTOptions.empty())) {

					int count = rnd.nextInt(0x2000); // 2^14

					long bound = 1L << i;

					double curr = bound * (rnd.nextDouble() - 0.5);
					List<ByteString> strings = new ArrayList<>(count);
					strings.add(ByteString.of(BigDecimal.valueOf(curr)));
					for (int j = 0; j < count; j++) {
						double vl = rnd.nextDouble() * bound;
						double prev = curr;
						curr += vl;
						if (Double.doubleToLongBits(curr) == Double.doubleToLongBits(prev))
							continue;

						strings.add(ByteString.of(BigDecimal.valueOf(curr)));
					}
					sec.load(strings.iterator(), strings.size(), ProgressListener.ignore());
					secEx.load(strings.iterator(), strings.size(), ProgressListener.ignore());

					Path idx = root.resolve("idx.bin");
					Path idx2 = root.resolve("idx2.bin");
					sec.save(idx, ProgressListener.ignore());
					secEx.save(idx2, ProgressListener.ignore());

					long l1 = Files.size(idx);
					long l2 = Files.size(idx2);
					System.out.println(l1 + " / " + l2 + " / " + (l1 * 10000 / l2) / 100.0);
				} catch (Throwable t) {
					try {
						IOUtil.deleteDirRecurse(root);
					} catch (IOException e) {
						t.addSuppressed(e);
					}
					throw t;
				}
			}

		}
		IOUtil.deleteDirRecurse(root);
	}

	@Test
	public void saveMapTest() throws IOException {
		Path root = tempDir.newFolder().toPath();
		Random rnd = new Random(456789);
		// iterations
		for (int z = 0; z < 4; z++) {
			// 50 to avoid reaching long limit
			for (int i = 2; i < 50; i++) {
				try (DictionarySectionPrivate sec = new DecimalDictionarySection(HDTOptions.empty())) {

					int count = rnd.nextInt(0x2000); // 2^14

					long bound = 1L << i;

					double curr = bound * (rnd.nextDouble() - 0.5);
					List<ByteString> strings = new ArrayList<>(count);
					strings.add(ByteString.of(BigDecimal.valueOf(curr)));
					for (int j = 0; j < count; j++) {
						double vl = rnd.nextDouble() * bound;
						double prev = curr;
						curr += vl;
						if (Double.doubleToLongBits(curr) == Double.doubleToLongBits(prev))
							continue;

						strings.add(ByteString.of(BigDecimal.valueOf(curr)));
					}
					sec.load(strings.iterator(), strings.size(), ProgressListener.ignore());

					Path idx = root.resolve("idx.bin");
					sec.save(idx, ProgressListener.ignore());

					try (CountInputStream is = new CountInputStream(new BufferedInputStream(Files.newInputStream(idx)));
							DecimalDictionarySectionMap sec2 = new DecimalDictionarySectionMap(is, idx.toFile())) {
						{

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
						}

						{ // test str
							Iterator<? extends CharSequence> it = sec2.getSortedEntries();
							Iterator<? extends CharSequence> itex = sec.getSortedEntries();
							long id = 0;
							while (it.hasNext()) {
								CharSequence its = it.next();
								id++;
								assertTrue("too many elements: " + id, itex.hasNext());
								CharSequence itsex = itex.next();
								assertEquals("bad elem: " + id, itsex, its);
								assertEquals(itsex, sec2.extract(id));
							}
							assertFalse("not enough elements " + id, itex.hasNext());
						}

						{
							Iterator<? extends CharSequence> it = sec2.getSortedEntries();
							long id = 0;
							while (it.hasNext()) {
								id++;
								CharSequence bs = it.next();
								assertEquals("bad idx for " + bs, id, sec2.locate(bs));
							}
						}

						{
							long id = 0;
							for (ByteString itsex : strings) {
								id++;
								assertEquals("bad idx for " + itsex, id, sec.locate(itsex));
							}
						}

					}
					Files.delete(idx);
				} catch (Throwable t) {
					try {
						IOUtil.deleteDirRecurse(root);
					} catch (IOException e) {
						t.addSuppressed(e);
					}
					throw t;
				}
			}

		}
		IOUtil.deleteDirRecurse(root);
	}

}

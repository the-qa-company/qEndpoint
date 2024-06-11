package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.IntCompactString;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntDictionarySectionTest {
	@Test
	public void loadTest() throws IOException {

		Random rnd = new Random(456789);
		// 50 to avoid reaching long limit
		for (int i = 2; i < 50; i++) {
			IntDictionarySection sec = new IntDictionarySection(HDTOptions.empty());

			int count = rnd.nextInt(0x2000); // 2^14

			long bound = 1L << i;

			long curr = rnd.nextLong(bound) - (bound / 2);
			List<ByteString> strings = new ArrayList<>(count);
			strings.add(ByteString.of(curr));
			for (int j = 0; j < count; j++) {
				long vl = rnd.nextLong(bound);
				if (vl == 0) continue;
				curr += vl;

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
package com.the_qa_company.qendpoint.core.triples.impl;

import com.the_qa_company.qendpoint.core.enums.ResultEstimationType;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.ControlInformation;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import org.apache.commons.io.file.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class BitmapQuadTriplesTest extends AbstractMapMemoryTest {
	private static IteratorTripleID fromList(List<TripleID> lst) {
		return new IteratorTripleID() {
			private int current;
			private int lastLoc;

			@Override
			public void goToStart() {
				current = 0;
			}

			@Override
			public boolean canGoTo() {
				return true;
			}

			@Override
			public void goTo(long pos) {
				current = (int) Math.min(lst.size(), Math.max(pos, 0));
			}

			@Override
			public long estimatedNumResults() {
				return lst.size();
			}

			@Override
			public ResultEstimationType numResultEstimation() {
				return ResultEstimationType.EXACT;
			}

			@Override
			public TripleComponentOrder getOrder() {
				return TripleComponentOrder.SPO;
			}

			@Override
			public long getLastTriplePosition() {
				return lastLoc;
			}

			@Override
			public boolean hasNext() {
				return current < lst.size();
			}

			@Override
			public TripleID next() {
				if (!hasNext()) {
					return null;
				}
				return lst.get(lastLoc = current++);
			}
		};
	}

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void triplesTest() throws IOException {
		Random rnd = new Random(5872);
		final long size = 10_000;

		List<TripleID> tripleIDList = new ArrayList<>();

		for (int i = 0; i < size; i++) {
			TripleID id = new TripleID();

			id.setSubject((i / 10) + 1);
			id.setPredicate(1 + rnd.nextInt((int) size / 100));
			id.setObject(1 + rnd.nextInt((int) size / 5));
			id.setGraph(1 + rnd.nextInt((int) size / 750));

			tripleIDList.add(id);
		}

		tripleIDList.sort(Comparator.comparing(TripleID::getSubject).thenComparingLong(TripleID::getPredicate)
				.thenComparingLong(TripleID::getObject).thenComparingLong(TripleID::getGraph));

		// remove dupes
		TripleID last = new TripleID();
		Iterator<TripleID> dupeIt = tripleIDList.iterator();

		while (dupeIt.hasNext()) {
			TripleID id = dupeIt.next();
			if (id.equals(last)) {
				dupeIt.remove();
				continue;
			}

			last.setAll(id.getSubject(), id.getPredicate(), id.getObject(), id.getGraph());
		}

		Path root = tempDir.newFolder().toPath();
		Path path = root.resolve("triples.bin");
		try {
			try (BitmapQuadTriples triples = new BitmapQuadTriples()) {
				triples.load(fromList(tripleIDList), ProgressListener.ignore());

				IteratorTripleID it = triples.searchAll();
				Iterator<TripleID> it2 = tripleIDList.iterator();

				while (it.hasNext()) {
					assertEquals(it2.next(), it.next());
				}

				try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(path))) {
					triples.save(stream, new ControlInformation(), ProgressListener.ignore());
				}
			}

			// load
			try (BitmapQuadTriples triples = new BitmapQuadTriples()) {
				try (InputStream stream = new BufferedInputStream(Files.newInputStream(path))) {
					ControlInformation ci = new ControlInformation();
					ci.load(stream);
					triples.load(stream, ci, ProgressListener.ignore());
				}

				IteratorTripleID it = triples.searchAll();
				Iterator<TripleID> it2 = tripleIDList.iterator();

				while (it.hasNext()) {
					assertEquals(it2.next(), it.next());
				}
			}

			// map
			try (BitmapQuadTriples triples = new BitmapQuadTriples()) {
				try (InputStream stream = new BufferedInputStream(Files.newInputStream(path))) {
					CountInputStream cstream = new CountInputStream(stream);
					triples.mapFromFile(cstream, path.toFile(), ProgressListener.ignore());
				}

				IteratorTripleID it = triples.searchAll();
				Iterator<TripleID> it2 = tripleIDList.iterator();

				while (it.hasNext()) {
					assertEquals(it2.next(), it.next());
				}
			}
		} finally {
			PathUtils.deleteDirectory(root);
		}
	}

}

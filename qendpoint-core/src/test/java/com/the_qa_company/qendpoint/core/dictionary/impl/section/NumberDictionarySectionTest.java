package com.the_qa_company.qendpoint.core.dictionary.impl.section;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySectionPrivate;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.DecimalCompactString;
import com.the_qa_company.qendpoint.core.util.string.DoubleCompactString;
import com.the_qa_company.qendpoint.core.util.string.IntCompactString;
import com.the_qa_company.qendpoint.core.util.string.NumberByteString;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore("speed things")
public class NumberDictionarySectionTest {
	private static final int COUNT = 10_000_000;
	@Test
	public void speedIntegerTest() throws IOException {
		Random rnd = new Random(34567);
		;
		List<? extends NumberByteString> valuesInt =
				LongStream.range(0, COUNT)
						.map(i -> rnd.nextInt(COUNT * 10))
						.sorted()
						.distinct()
						.mapToObj(IntCompactString::new)
						.toList();

		List<ByteString> valuesStr = valuesInt.stream()
				.map(NumberByteString::asByteString)
				.sorted()
				.toList();

		try (
				DictionarySectionPrivate seci = new IntDictionarySection(HDTOptions.empty());
				DictionarySectionPrivate secs = new PFCDictionarySectionBig(HDTOptions.empty());
		) {
			StopWatch sw = new StopWatch();
			sw.reset();
			secs.load(valuesStr.iterator(), valuesStr.size(), ProgressListener.ignore());
			System.out.println("Load strings: " + sw.stopAndShow());
			// IteratorUtils.printHead(secs.getSortedEntries(), 100);
			sw.reset();
			seci.load(valuesInt.iterator(), valuesInt.size(), ProgressListener.ignore());
			System.out.println("Load ints: " + sw.stopAndShow());
			// IteratorUtils.printHead(seci.getSortedEntries(), 100);



			sw.reset();
			{
				Iterator<? extends CharSequence> it = secs.getSortedEntries();
				Iterator<? extends ByteString> itex = valuesStr.iterator();

				while (it.hasNext()) {
					assertTrue(itex.hasNext());
					ByteString ac = ByteString.of(it.next());
					ByteString ex = itex.next();

					assertEquals(ex, ac);
				}
				assertFalse(itex.hasNext());
				System.out.println("str: " + sw.stopAndShow());
			}

			sw.reset();
			{
				Iterator<? extends CharSequence> it = seci.getSortedEntries();
				Iterator<? extends NumberByteString> itex = valuesInt.iterator();

				while (it.hasNext()) {
					assertTrue(itex.hasNext());
					ByteString ac = ((NumberByteString)it.next()).asByteString();
					ByteString ex = itex.next().asByteString();

					assertEquals(ex, ac);
				}
				assertFalse(itex.hasNext());
				System.out.println("int: " + sw.stopAndShow());
			}


		}

	}

	@Test
	public void speedDoubleTest() throws IOException {
		Random rnd = new Random(34567);
		List<? extends NumberByteString> valuesInt =
				LongStream.range(0, COUNT)
						.mapToDouble(i -> rnd.nextDouble())
						.sorted()
						.distinct()
						.mapToObj(DoubleCompactString::new)
						.toList();

		List<ByteString> valuesStr = valuesInt.stream()
				.map(NumberByteString::asByteString)
				.sorted()
				.toList();

		try (
				DictionarySectionPrivate seci = new FloatDictionarySection(HDTOptions.empty());
				DictionarySectionPrivate secs = new PFCDictionarySectionBig(HDTOptions.empty());
		) {
			StopWatch sw = new StopWatch();
			sw.reset();
			secs.load(valuesStr.iterator(), valuesStr.size(), ProgressListener.ignore());
			System.out.println("Load strings: " + sw.stopAndShow());
			// IteratorUtils.printHead(secs.getSortedEntries(), 100);
			sw.reset();
			seci.load(valuesInt.iterator(), valuesInt.size(), ProgressListener.ignore());
			System.out.println("Load ints: " + sw.stopAndShow());
			// IteratorUtils.printHead(seci.getSortedEntries(), 100);



			sw.reset();
			{
				Iterator<? extends CharSequence> it = secs.getSortedEntries();
				Iterator<? extends ByteString> itex = valuesStr.iterator();

				while (it.hasNext()) {
					assertTrue(itex.hasNext());
					ByteString ac = ByteString.of(it.next());
					ByteString ex = itex.next();

					assertEquals(ex, ac);
				}
				assertFalse(itex.hasNext());
				System.out.println("str: " + sw.stopAndShow());
			}

			sw.reset();
			{
				Iterator<? extends CharSequence> it = seci.getSortedEntries();
				Iterator<? extends NumberByteString> itex = valuesInt.iterator();

				while (it.hasNext()) {
					assertTrue(itex.hasNext());
					ByteString ac = ((NumberByteString)it.next()).asByteString();
					ByteString ex = itex.next().asByteString();

					assertEquals(ex, ac);
				}
				assertFalse(itex.hasNext());
				System.out.println("int: " + sw.stopAndShow());
			}


		}

	}

	@Test
	public void speedDecimalTest() throws IOException {
		Random rnd = new Random(34567);
		List<? extends NumberByteString> valuesInt =
				LongStream.range(0, COUNT)
						.mapToObj(i -> BigDecimal.valueOf(rnd.nextDouble()))
						.sorted()
						.distinct()
						.map(DecimalCompactString::new)
						.toList();

		List<ByteString> valuesStr = valuesInt.stream()
				.map(NumberByteString::asByteString)
				.sorted()
				.toList();

		try (
				DictionarySectionPrivate seci = new DecimalDictionarySection(HDTOptions.empty());
				DictionarySectionPrivate secs = new PFCDictionarySectionBig(HDTOptions.empty());
		) {
			StopWatch sw = new StopWatch();
			sw.reset();
			secs.load(valuesStr.iterator(), valuesStr.size(), ProgressListener.ignore());
			System.out.println("Load strings: " + sw.stopAndShow());
			// IteratorUtils.printHead(secs.getSortedEntries(), 100);
			sw.reset();
			seci.load(valuesInt.iterator(), valuesInt.size(), ProgressListener.ignore());
			System.out.println("Load ints: " + sw.stopAndShow());
			// IteratorUtils.printHead(seci.getSortedEntries(), 100);



			sw.reset();
			{
				Iterator<? extends CharSequence> it = secs.getSortedEntries();
				Iterator<? extends ByteString> itex = valuesStr.iterator();

				while (it.hasNext()) {
					assertTrue(itex.hasNext());
					ByteString ac = ByteString.of(it.next());
					ByteString ex = itex.next();

					assertEquals(ex, ac);
				}
				assertFalse(itex.hasNext());
				System.out.println("str: " + sw.stopAndShow());
			}

			sw.reset();
			{
				Iterator<? extends CharSequence> it = seci.getSortedEntries();
				Iterator<? extends NumberByteString> itex = valuesInt.iterator();

				while (it.hasNext()) {
					assertTrue(itex.hasNext());
					ByteString ac = ((NumberByteString)it.next()).asByteString();
					ByteString ex = itex.next().asByteString();

					assertEquals(ex, ac);
				}
				assertFalse(itex.hasNext());
				System.out.println("int: " + sw.stopAndShow());
			}


		}

	}
}

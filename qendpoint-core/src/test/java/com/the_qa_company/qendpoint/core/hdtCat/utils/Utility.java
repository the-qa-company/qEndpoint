package com.the_qa_company.qendpoint.core.hdtCat.utils;

import com.the_qa_company.qendpoint.core.dictionary.Dictionary;
import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import org.junit.Assert;

import java.util.Iterator;
import java.util.Map;

public class Utility {

	public static void printDictionary(Dictionary d) {
		Iterator<? extends CharSequence> shared = d.getShared().getSortedEntries();
		int count = 0;
		System.out.println("SHARED");
		while (shared.hasNext()) {
			System.out.println(count + "---" + shared.next().toString());
			count++;
		}
		System.out.println("SUBJECTS");
		count = 0;
		Iterator<? extends CharSequence> subjects = d.getSubjects().getSortedEntries();
		while (subjects.hasNext()) {
			System.out.println(count + "---" + subjects.next().toString());
			count++;
		}
		System.out.println("OBJECTS");
		count = 0;
		Iterator<? extends CharSequence> objects = d.getObjects().getSortedEntries();
		while (objects.hasNext()) {
			System.out.println(count + "---" + objects.next().toString());
			count++;
		}
		System.out.println("PREDICATES");
		count = 0;
		Iterator<? extends CharSequence> predicates = d.getPredicates().getSortedEntries();
		while (predicates.hasNext()) {
			System.out.println(count + "---" + predicates.next().toString());
			count++;
		}
	}

	public static void printCustomDictionary(Dictionary d) {
		Iterator<? extends CharSequence> shared = d.getShared().getSortedEntries();
		int count = 0;
		System.out.println("SHARED");
		while (shared.hasNext()) {
			System.out.println(count + "---" + shared.next().toString());
			count++;
		}
		System.out.println("SUBJECTS");
		count = 0;
		Iterator<? extends CharSequence> subjects = d.getSubjects().getSortedEntries();
		while (subjects.hasNext()) {
			System.out.println(count + "---" + subjects.next().toString());
			count++;
		}
		System.out.println("OBJECTS");
		count = 0;
		for (Map.Entry<? extends CharSequence, DictionarySection> stringDictionarySectionEntry : d.getAllObjects()
				.entrySet()) {
			Iterator<? extends CharSequence> entries = stringDictionarySectionEntry.getValue().getSortedEntries();
			while (entries.hasNext()) {
				System.out.println(count + "---" + entries.next().toString());
				count++;
			}
		}
		System.out.println("PREDICATES");
		count = 0;
		subjects = d.getPredicates().getSortedEntries();
		while (subjects.hasNext()) {
			System.out.println(count + "---" + subjects.next().toString());
			count++;
		}
	}

	public static void assertEquals(Iterator<? extends CharSequence> i1, Iterator<? extends CharSequence> i2) {
		while (i1.hasNext()) {
			Assert.assertTrue("The dictionaries have a different size", i2.hasNext());
			Assert.assertEquals(i1.next().toString(), i2.next().toString());
		}
		Assert.assertFalse("The dictionaries have a different size", i2.hasNext());
	}

	public static void compareDictionary(Dictionary d1, Dictionary d2) {
		assertEquals(d1.getShared().getSortedEntries(), d2.getShared().getSortedEntries());
		assertEquals(d1.getSubjects().getSortedEntries(), d2.getSubjects().getSortedEntries());
		assertEquals(d1.getObjects().getSortedEntries(), d2.getObjects().getSortedEntries());
		assertEquals(d1.getPredicates().getSortedEntries(), d2.getPredicates().getSortedEntries());
	}

	public static void compareCustomDictionary(Dictionary d1, Dictionary d2) {
		assertEquals(d1.getShared().getSortedEntries(), d2.getShared().getSortedEntries());
		assertEquals(d1.getSubjects().getSortedEntries(), d2.getSubjects().getSortedEntries());
		Iterator<? extends Map.Entry<? extends CharSequence, DictionarySection>> hmIter1 = d1.getAllObjects().entrySet()
				.iterator();
		Iterator<? extends Map.Entry<? extends CharSequence, DictionarySection>> hmIter2 = d2.getAllObjects().entrySet()
				.iterator();
		while (hmIter1.hasNext()) {
			Assert.assertTrue("The dictionaries have a different number of objects subsections", hmIter2.hasNext());
			Map.Entry<? extends CharSequence, DictionarySection> entry1 = hmIter1.next();
			Map.Entry<? extends CharSequence, DictionarySection> entry2 = hmIter2.next();

			assertEquals(entry1.getValue().getSortedEntries(), entry2.getValue().getSortedEntries());
		}
		Assert.assertFalse("The dictionaries have a different number of objects subsections", hmIter2.hasNext());
		assertEquals(d1.getPredicates().getSortedEntries(), d2.getPredicates().getSortedEntries());
	}

	public static void printTriples(HDT hdt) {
		try {
			hdt.search("", "", "").forEachRemaining(
					s -> System.out.println(s.getSubject() + "--" + s.getPredicate() + "--" + s.getObject()));
		} catch (NotFoundException e) {
			throw new AssertionError(e);
		}
	}

	public static void compareTriples(HDT hdt1, HDT hdt2) {
		IteratorTripleID itOld = hdt1.getTriples().searchAll();
		IteratorTripleID itNew = hdt2.getTriples().searchAll();
		while (itOld.hasNext()) {
			Assert.assertTrue("The number of triples is different", itNew.hasNext());
			TripleID tripleIDOld = itOld.next();
			String subjectOld = hdt1.getDictionary().idToString(tripleIDOld.getSubject(), TripleComponentRole.SUBJECT)
					.toString();
			String predicateOld = hdt1.getDictionary()
					.idToString(tripleIDOld.getPredicate(), TripleComponentRole.PREDICATE).toString();
			String objectOld = hdt1.getDictionary().idToString(tripleIDOld.getObject(), TripleComponentRole.OBJECT)
					.toString();
			TripleID tripleIDNew = itNew.next();
			String subjectNew = hdt2.getDictionary().idToString(tripleIDNew.getSubject(), TripleComponentRole.SUBJECT)
					.toString();
			String predicateNew = hdt2.getDictionary()
					.idToString(tripleIDNew.getPredicate(), TripleComponentRole.PREDICATE).toString();
			String objectNew = hdt2.getDictionary().idToString(tripleIDNew.getObject(), TripleComponentRole.OBJECT)
					.toString();
			Assert.assertEquals(subjectOld, subjectNew);
			Assert.assertEquals(predicateOld, predicateNew);
			Assert.assertEquals(objectOld, objectNew);
		}
		Assert.assertFalse("The number of triples is different", itNew.hasNext());
	}

	/**
	 * utility class to create closable from non closeable object
	 *
	 * @param object         the object to close
	 * @param closeOperation close operation
	 * @return closeable
	 * @param <T> object type
	 * @param <E> close exception type
	 */
	public static <T, E extends Exception> PackAutoCloseable<T, E> closeableOf(T object,
			CloseOperation<T, E> closeOperation) {
		return new PackAutoCloseable<>(object, closeOperation);
	}

	public static class PackAutoCloseable<T, E extends Exception> implements AutoCloseable {
		private final T object;
		private final CloseOperation<T, E> closeOperation;

		private PackAutoCloseable(T object, CloseOperation<T, E> closeOperation) {
			this.object = object;
			this.closeOperation = closeOperation;
		}

		public T getObject() {
			return object;
		}

		@Override
		public void close() throws E {
			closeOperation.close(object);
		}
	}

	public interface CloseOperation<T, E extends Exception> {
		void close(T object) throws E;
	}
}

package com.the_qa_company.qendpoint.core.iterator.charsequence;

import com.the_qa_company.qendpoint.core.iterator.utils.FetcherIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.PeekIterator;
import com.the_qa_company.qendpoint.core.util.LiteralsUtils;

/**
 * Iterators stopping when a pattern isn't matched anymore
 *
 * @param <T>
 */
public abstract class StopIterator<T extends CharSequence> extends FetcherIterator<T> {
	private static class LangStopIterator<T extends CharSequence> extends StopIterator<T> {
		private final CharSequence language;

		private LangStopIterator(PeekIterator<T> it, CharSequence language) {
			super(it);
			this.language = language;
		}

		@Override
		protected T getNext() {
			if (!peek.hasNext()) {
				return null; // end
			}
			T elem = peek.peek();
			CharSequence type = LiteralsUtils.getType(elem);
			if (type != LiteralsUtils.LITERAL_LANG_TYPE) {
				return null; // not a lang
			}

			CharSequence lg = LiteralsUtils.getLanguage(elem).orElseThrow();
			if (!lg.equals(language)) {
				return null; // not the same language
			}
			return peek.next(); // we can send the next element
		}
	}

	private static class TypeStopIterator<T extends CharSequence> extends StopIterator<T> {
		private final CharSequence type;

		private TypeStopIterator(PeekIterator<T> it, CharSequence type) {
			super(it);
			this.type = type;
		}

		@Override
		protected T getNext() {
			if (!peek.hasNext()) {
				return null; // end
			}
			T elem = peek.peek();
			CharSequence type = LiteralsUtils.getType(elem);
			if (!type.equals(this.type)) {
				return null; // not the same type
			}
			return peek.next(); // we can send the next element
		}
	}

	private static class NoTypeStopIterator<T extends CharSequence> extends StopIterator<T> {

		private NoTypeStopIterator(PeekIterator<T> it) {
			super(it);
		}

		@Override
		protected T getNext() {
			if (!peek.hasNext()) {
				return null; // end
			}
			T elem = peek.peek();
			CharSequence type = LiteralsUtils.getType(elem);
			if (type != LiteralsUtils.NO_DATATYPE) {
				return null; // not the same type
			}
			return peek.next(); // we can send the next element
		}
	}

	private static class CountStopIterator<T extends CharSequence> extends StopIterator<T> {
		private final long count;
		private long index;

		private CountStopIterator(PeekIterator<T> it, long count) {
			super(it);
			this.count = count;
		}

		@Override
		protected T getNext() {
			if (index++ >= count) {
				return null;
			}
			if (!peek.hasNext()) {
				throw new IllegalArgumentException("bad count: " + count + ">" + index);
			}
			return peek.next(); // we can send the next element
		}
	}

	/**
	 * stop iterator that runs until a count of element
	 *
	 * @param it    peek iterator
	 * @param count count
	 * @param <T>   iterator type
	 * @return iterator
	 */
	public static <T extends CharSequence> PeekIterator<T> count(PeekIterator<T> it, long count) {
		return new CountStopIterator<>(it, count);
	}

	/**
	 * stop iterator that runs until the language is the same
	 *
	 * @param it       peek iterator
	 * @param language language
	 * @param <T>      iterator type
	 * @return iterator
	 */
	public static <T extends CharSequence> PeekIterator<T> language(PeekIterator<T> it, CharSequence language) {
		return new LangStopIterator<>(it, language);
	}

	/**
	 * stop iterator that runs until the type is the same
	 *
	 * @param it   peek iterator
	 * @param type type
	 * @param <T>  iterator type
	 * @return iterator
	 */
	public static <T extends CharSequence> PeekIterator<T> typed(PeekIterator<T> it, CharSequence type) {
		return new TypeStopIterator<>(it, type);
	}

	/**
	 * stop iterator that runs until a typed literal is found
	 *
	 * @param it  peek iterator
	 * @param <T> iterator type
	 * @return iterator
	 */
	public static <T extends CharSequence> PeekIterator<T> noType(PeekIterator<T> it) {
		return new NoTypeStopIterator<>(it);
	}

	protected final PeekIterator<T> peek;

	private StopIterator(PeekIterator<T> it) {
		peek = it;
	}
}

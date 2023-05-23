package com.the_qa_company.qendpoint.core.storage.merge;

import com.the_qa_company.qendpoint.core.dictionary.DictionarySection;
import com.the_qa_company.qendpoint.core.storage.QEPDatasetContext;
import com.the_qa_company.qendpoint.core.util.BitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToLongFunction;

/**
 * Merger function to apply a numerical value to a dataset
 *
 * @author Antoine Willerval
 */
public enum MergeFindFunc {
	SIZE_LOG(ds -> BitUtil.log2(ds.dataset().dataset().getTriples().getNumberOfElements()), true),
	SIZE(ds -> ds.dataset().dataset().getTriples().getNumberOfElements()),
	DICT_SIZE(ds -> ds.dataset().dataset().getDictionary().getAllObjects().values().stream()
			.mapToLong(DictionarySection::getNumberOfElements).sum()),
	DICT_SIZE_LOG(ds -> BitUtil.log2(ds.dataset().dataset().getDictionary().getAllObjects().values().stream()
			.mapToLong(DictionarySection::getNumberOfElements).sum()));

	private static final Map<String, MergeFindFunc> MAP = new HashMap<>();
	private static final Logger logger = LoggerFactory.getLogger(MergeFindFunc.class);
	private static final MergeFindFunc DEFAULT_FUNC;

	static {
		MergeFindFunc dft = null;
		for (MergeFindFunc v : values()) {
			MAP.put(v.name(), v);
			if (v.defaultValue) {
				dft = v;
			}
		}
		if (dft == null) {
			dft = values()[0];
		}
		DEFAULT_FUNC = dft;
	}

	/**
	 * read an option value, if it matches the {@link #name()} of a function
	 * (non-case-sensitive), it'll return the function, otherwise I'll return
	 * the default function
	 *
	 * @param opt option to read, nullable
	 * @return function
	 */
	public static MergeFindFunc readOption(String opt) {
		if (opt == null || opt.isEmpty()) {
			return getDefaultFunc();
		}
		MergeFindFunc val = MAP.get(opt.toLowerCase());
		if (val == null) {
			logger.warn("Can't find {} with the name '{}'", MergeFindFunc.class, opt);
			return getDefaultFunc();
		}
		return val;
	}

	/**
	 * @return the default function
	 */
	public static MergeFindFunc getDefaultFunc() {
		return DEFAULT_FUNC;
	}

	private final boolean defaultValue;
	private final ToLongFunction<QEPDatasetContext> func;
	private final Comparator<QEPDatasetContext> comparator;

	MergeFindFunc(ToLongFunction<QEPDatasetContext> func) {
		this(func, false);
	}

	MergeFindFunc(ToLongFunction<QEPDatasetContext> func, boolean defaultValue) {
		this.defaultValue = defaultValue;
		this.func = func;
		comparator = Comparator.comparingLong(func);
	}

	/**
	 * @return get a comparator to compare contexts
	 */
	public Comparator<QEPDatasetContext> getComparator() {
		return comparator;
	}

	/**
	 * map a context to a long value
	 *
	 * @param ctx the context
	 * @return long
	 */
	public long mapValue(QEPDatasetContext ctx) {
		return func.applyAsLong(ctx);
	}
}

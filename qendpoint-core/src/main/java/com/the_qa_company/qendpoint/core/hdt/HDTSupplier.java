package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface describing an HDT generator method
 *
 * @author Antoine Willerval
 */
@FunctionalInterface
public interface HDTSupplier {
	Map<String, HDTSupplier> SUPPLIERS = new HashMap<>() {
		{
			put(HDTOptionsKeys.LOADER_CATTREE_HDT_SUPPLIER_VALUE_MEMORY, memory());
			put(HDTOptionsKeys.LOADER_CATTREE_HDT_SUPPLIER_VALUE_DISK, disk());
		}
	};

	/**
	 * @return implementation using in-memory hdt
	 */
	static HDTSupplier memory() {
		return (iterator, baseURI, hdtFormat, listener, location) -> {
			try (HDT hdt = HDTManager.generateHDT(iterator, baseURI, hdtFormat, listener)) {
				hdt.saveToHDT(location.toAbsolutePath().toString(), listener);
			}
		};
	}

	/**
	 * @return implementation using in-memory hdt
	 */
	static HDTSupplier disk() {
		return (iterator, baseURI, hdtFormat, listener, location) -> {
			hdtFormat.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, location.toAbsolutePath().toString());
			HDTManager.generateHDTDisk(iterator, baseURI, hdtFormat, listener).close();
		};
	}

	/**
	 * create a HDTSupplier from spec
	 *
	 * @param spec the specs
	 * @return hdt supplier
	 */
	static HDTSupplier fromSpec(HDTOptions spec) {
		if (spec == null) {
			return memory();
		}
		String supplier = spec.get(HDTOptionsKeys.HDT_SUPPLIER_KEY);

		if (supplier == null || supplier.isEmpty()) {
			return memory();
		}

		HDTSupplier s = SUPPLIERS.get(supplier);

		if (s == null) {
			throw new IllegalArgumentException("Can't find a supplier for name: " + supplier);
		}

		return s;
	}

	/**
	 * Generate the HDT
	 *
	 * @param iterator  the iterator to create the hdt
	 * @param baseURI   the base URI (useless, but asked by some methods)
	 * @param hdtFormat the HDT options to create the HDT
	 * @param listener  listener
	 * @param location  where to write the HDT
	 * @throws IOException     io exception while creating the HDT
	 * @throws ParserException parser exception while retrieving the triples
	 */
	void doGenerateHDT(Iterator<TripleString> iterator, String baseURI, HDTOptions hdtFormat, ProgressListener listener,
			Path location) throws IOException, ParserException;
}

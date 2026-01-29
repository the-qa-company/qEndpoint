package com.the_qa_company.qendpoint.core.tools;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RDF2HDTThreadingTest {
	@Test
	public void applyThreadingOverridesForcesSingleThreadWhenDisabled() throws Throwable {
		RDF2HDT rdf2hdt = new RDF2HDT();
		rdf2hdt.multiThreadLog = false;

		HDTOptions spec = HDTOptions.of();
		spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "4");
		spec.set(HDTOptionsKeys.LOADER_DISK_MERGE_CONCURRENCY_KEY, "3");
		spec.set(HDTOptionsKeys.BITMAPTRIPLES_OBJECT_INDEX_PARALLELISM_KEY, "7");
		spec.set(HDTOptionsKeys.PARSER_RIOT_PARALLEL_KEY, "true");

		invokeApplyThreadingOverrides(rdf2hdt, spec);

		assertEquals(4, spec.getInt(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, 0));
		assertEquals(3, spec.getInt(HDTOptionsKeys.LOADER_DISK_MERGE_CONCURRENCY_KEY, 0));
		assertEquals(7, spec.getInt(HDTOptionsKeys.BITMAPTRIPLES_OBJECT_INDEX_PARALLELISM_KEY, 0));
		assertEquals(true, spec.getBoolean(HDTOptionsKeys.PARSER_RIOT_PARALLEL_KEY, false));
	}

	@Test
	public void applyThreadingOverridesKeepsConfiguredParallelismWhenEnabled() throws Throwable {
		RDF2HDT rdf2hdt = new RDF2HDT();
		rdf2hdt.multiThreadLog = true;

		HDTOptions spec = HDTOptions.of();
		spec.set(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, "6");
		spec.set(HDTOptionsKeys.LOADER_DISK_MERGE_CONCURRENCY_KEY, "5");
		spec.set(HDTOptionsKeys.BITMAPTRIPLES_OBJECT_INDEX_PARALLELISM_KEY, "4");
		spec.set(HDTOptionsKeys.PARSER_RIOT_PARALLEL_KEY, "false");

		invokeApplyThreadingOverrides(rdf2hdt, spec);

		assertEquals(6, spec.getInt(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY, 0));
		assertEquals(5, spec.getInt(HDTOptionsKeys.LOADER_DISK_MERGE_CONCURRENCY_KEY, 0));
		assertEquals(4, spec.getInt(HDTOptionsKeys.BITMAPTRIPLES_OBJECT_INDEX_PARALLELISM_KEY, 0));
		assertEquals(false, spec.getBoolean(HDTOptionsKeys.PARSER_RIOT_PARALLEL_KEY, true));
	}

	private static void invokeApplyThreadingOverrides(RDF2HDT rdf2hdt, HDTOptions spec) throws Throwable {
		Method method = RDF2HDT.class.getDeclaredMethod("applyThreadingOverrides", HDTOptions.class);
		method.setAccessible(true);
		method.invoke(rdf2hdt, spec);
	}
}

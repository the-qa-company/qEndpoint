package com.the_qa_company.qendpoint.core.rdf;

import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RDFFluxStopTest {
	private void assertExportSame(RDFFluxStop flux) {
		String cfg = flux.asConfig();
		RDFFluxStop readCfg = RDFFluxStop.readConfig(cfg);
		assertEquals(flux, readCfg);
		String cfg2 = readCfg.asConfig();
		assertEquals(cfg, cfg2);
		RDFFluxStop readCfg2 = RDFFluxStop.readConfig(cfg);
		assertEquals(flux, readCfg2);
	}

	@Test
	public void optionTest() {
		assertEquals(HDTOptionsKeys.RDF_FLUX_STOP_VALUE_NO_LIMIT + ":0", RDFFluxStop.noLimit().asConfig());
		assertEquals(HDTOptionsKeys.RDF_FLUX_STOP_VALUE_COUNT + ":42", RDFFluxStop.countLimit(42).asConfig());
		assertEquals(HDTOptionsKeys.RDF_FLUX_STOP_VALUE_SIZE + ":34", RDFFluxStop.sizeLimit(34).asConfig());

		assertEquals("()!(" + HDTOptionsKeys.RDF_FLUX_STOP_VALUE_SIZE + ":34)",
				RDFFluxStop.sizeLimit(34).not().asConfig());

		assertEquals("(" + HDTOptionsKeys.RDF_FLUX_STOP_VALUE_COUNT + ":42)&(" + HDTOptionsKeys.RDF_FLUX_STOP_VALUE_SIZE
				+ ":34)", RDFFluxStop.countLimit(42).and(RDFFluxStop.sizeLimit(34)).asConfig());
		assertEquals("(" + HDTOptionsKeys.RDF_FLUX_STOP_VALUE_COUNT + ":42)|(" + HDTOptionsKeys.RDF_FLUX_STOP_VALUE_SIZE
				+ ":34)", RDFFluxStop.countLimit(42).or(RDFFluxStop.sizeLimit(34)).asConfig());
		assertEquals(
				RDFFluxStop.readConfig("(" + HDTOptionsKeys.RDF_FLUX_STOP_VALUE_COUNT + ":42)&("
						+ HDTOptionsKeys.RDF_FLUX_STOP_VALUE_SIZE + ":34)"),
				RDFFluxStop.countLimit(42).and(RDFFluxStop.sizeLimit(34)));
		assertEquals(
				RDFFluxStop.readConfig("(" + HDTOptionsKeys.RDF_FLUX_STOP_VALUE_COUNT + ":42)|("
						+ HDTOptionsKeys.RDF_FLUX_STOP_VALUE_SIZE + ":34)"),
				RDFFluxStop.countLimit(42).or(RDFFluxStop.sizeLimit(34)));

		assertExportSame(RDFFluxStop.countLimit(42).or(RDFFluxStop.sizeLimit(34)));
		assertExportSame(RDFFluxStop.countLimit(42).or(RDFFluxStop.sizeLimit(34)).not());
		assertExportSame((RDFFluxStop.countLimit(42).and(RDFFluxStop.countLimit(1)))
				.or(RDFFluxStop.sizeLimit(34).not().and(RDFFluxStop.noLimit())).and(RDFFluxStop.countLimit(23)));

		assertNull(RDFFluxStop.readConfig(""));
		assertNull(RDFFluxStop.readConfig(null));
		assertNull(RDFFluxStop.readConfig("()"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void badSyntax() {
		RDFFluxStop.readConfig("noLimit");
	}

	@Test(expected = IllegalArgumentException.class)
	public void badSyntax2() {
		RDFFluxStop.readConfig("noLimit:2&z");
	}

	@Test(expected = IllegalArgumentException.class)
	public void badSyntax3() {
		RDFFluxStop.readConfig("''");
	}

	@Test(expected = IllegalArgumentException.class)
	public void badSyntax4() {
		RDFFluxStop.readConfig("a:b");
	}
}

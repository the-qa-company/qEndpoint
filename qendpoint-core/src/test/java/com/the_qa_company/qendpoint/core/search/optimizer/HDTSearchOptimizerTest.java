package com.the_qa_company.qendpoint.core.search.optimizer;

import com.the_qa_company.qendpoint.core.search.AbstractQueryTest;
import com.the_qa_company.qendpoint.core.search.component.HDTComponentTriple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HDTSearchOptimizerTest extends AbstractQueryTest {
	private final Optimizer optimizer = HDTSearchOptimizer.getInstance();

	@Test
	public void optimizerTest() {
		List<HDTComponentTriple> patterns = new ArrayList<>(
				List.of(triple("?s", "<p>", "?o"), triple("?s", "<p2>", "<o2>"), triple("<s3>", "<p3>", "?o"),
						triple("<s4>", "?p", "?o2"), triple("?s", "<p4>", "?o3"), triple("?s", "?p", "?o")));
		optimizer.optimize(patterns);
		assertEquals(List.of(triple("<s3>", "<p3>", "?o"), triple("?s", "<p>", "?o"), triple("?s", "<p2>", "<o2>"),
				triple("?s", "<p4>", "?o3"), triple("<s4>", "?p", "?o2"), triple("?s", "?p", "?o")), patterns);
	}
}

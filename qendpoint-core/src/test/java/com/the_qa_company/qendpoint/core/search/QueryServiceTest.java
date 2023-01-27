package com.the_qa_company.qendpoint.core.search;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class QueryServiceTest extends AbstractQueryTest {
	@Test
	public void serviceTest() {
		assertNotNull("CORE factory wasn't found!", FACTORY);
	}
}

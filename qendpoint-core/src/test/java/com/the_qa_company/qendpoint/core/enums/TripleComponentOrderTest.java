package com.the_qa_company.qendpoint.core.enums;

import org.junit.Test;

import static org.junit.Assert.*;

public class TripleComponentOrderTest {

	@Test
	public void mappingTest() {
		assertEquals(TripleComponentOrder.SPO.getSubjectInvMapping(), TripleComponentRole.SUBJECT);
		assertEquals(TripleComponentOrder.SPO.getPredicateInvMapping(), TripleComponentRole.PREDICATE);
		assertEquals(TripleComponentOrder.SPO.getObjectInvMapping(), TripleComponentRole.OBJECT);

		assertEquals(TripleComponentOrder.SOP.getSubjectInvMapping(), TripleComponentRole.SUBJECT);
		assertEquals(TripleComponentOrder.SOP.getPredicateInvMapping(), TripleComponentRole.OBJECT);
		assertEquals(TripleComponentOrder.SOP.getObjectInvMapping(), TripleComponentRole.PREDICATE);

		assertEquals(TripleComponentOrder.OPS.getSubjectInvMapping(), TripleComponentRole.OBJECT);
		assertEquals(TripleComponentOrder.OPS.getPredicateInvMapping(), TripleComponentRole.PREDICATE);
		assertEquals(TripleComponentOrder.OPS.getObjectInvMapping(), TripleComponentRole.SUBJECT);

		assertEquals(TripleComponentOrder.OSP.getSubjectInvMapping(), TripleComponentRole.PREDICATE);
		assertEquals(TripleComponentOrder.OSP.getPredicateInvMapping(), TripleComponentRole.OBJECT);
		assertEquals(TripleComponentOrder.OSP.getObjectInvMapping(), TripleComponentRole.SUBJECT);

		assertEquals(TripleComponentOrder.PSO.getSubjectInvMapping(), TripleComponentRole.PREDICATE);
		assertEquals(TripleComponentOrder.PSO.getPredicateInvMapping(), TripleComponentRole.SUBJECT);
		assertEquals(TripleComponentOrder.PSO.getObjectInvMapping(), TripleComponentRole.OBJECT);

		assertEquals(TripleComponentOrder.POS.getSubjectInvMapping(), TripleComponentRole.OBJECT);
		assertEquals(TripleComponentOrder.POS.getPredicateInvMapping(), TripleComponentRole.SUBJECT);
		assertEquals(TripleComponentOrder.POS.getObjectInvMapping(), TripleComponentRole.PREDICATE);

	}

}

package com.the_qa_company.qendpoint.model;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.base.CoreDatatype;

import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

public class RawNumberLiteralHDT implements Literal, HDTValue {

	private final long hdtId;

	public RawNumberLiteralHDT(long hdtId) {
		this.hdtId = hdtId;
	}

	@Override
	public String getLabel() {
		return null;
	}

	@Override
	public Optional<String> getLanguage() {
		return Optional.empty();
	}

	@Override
	public IRI getDatatype() {
		return null;
	}

	@Override
	public boolean booleanValue() {
		return false;
	}

	@Override
	public byte byteValue() {
		return 0;
	}

	@Override
	public short shortValue() {
		return 0;
	}

	@Override
	public int intValue() {
		return 0;
	}

	@Override
	public long longValue() {
		return 0;
	}

	@Override
	public BigInteger integerValue() {
		return null;
	}

	@Override
	public BigDecimal decimalValue() {
		return null;
	}

	@Override
	public float floatValue() {
		return 0;
	}

	@Override
	public double doubleValue() {
		return 0;
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		return null;
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		return null;
	}

	@Override
	public String stringValue() {
		return null;
	}

	@Override
	public boolean isDelegate() {
		return false;
	}

	@Override
	public void setDelegate(boolean delegate) {

	}

	@Override
	public long getHDTId() {
		return hdtId;
	}

	@Override
	public int getHDTPosition() {
		return SimpleIRIHDT.OBJECT_POS;
	}
}

/**
 * *****************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others. All rights
 * reserved. This program and the accompanying materials are made available
 * under the terms of the Eclipse Distribution License v1.0 which accompanies
 * this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 * *****************************************************************************
 */
package com.the_qa_company.qendpoint.model;

import com.the_qa_company.qendpoint.core.util.LiteralsUtils;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.ByteStringUtil;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.vocabulary.XSD;
import org.eclipse.rdf4j.rio.helpers.NTriplesUtil;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.hdt.HDT;

import javax.xml.datatype.XMLGregorianCalendar;
import java.io.Serial;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

import static org.eclipse.rdf4j.rio.helpers.NTriplesUtil.parseURI;

/**
 * A implementation of the {@link Literal} interface for HDT.
 *
 * @author Dennis Diefenbach
 */
public class SimpleLiteralHDT implements Literal, HDTValue {

	/*-----------*
	 * Constants *
	 *-----------*/

	@Serial
	private static final long serialVersionUID = -1649571784782592271L;

	/*-----------*
	 * Variables *
	 *-----------*/

	/**
	 * The literal's hdt ID.
	 */
	private HDT hdt;

	private final ValueFactory valueFactory;

	/**
	 * The literal's hdt ID.
	 */
	private long hdtID;

	/**
	 * The literal's label.
	 */
	private String labelStr;
	private ByteString label;

	/**
	 * The literal's language tag.
	 */
	private Optional<String> language;

	/**
	 * The literal's datatype.
	 */
	private IRI datatype;

	private CoreDatatype coreDatatype;

	/*--------------*
	 * Constructors *
	 *--------------*/

	/**
	 * Creates a new plain literal with the supplied label.
	 *
	 * @param hdt The hdt file in which the literal is contained, must not be
	 *            <tt>null</tt>.
	 */
	public SimpleLiteralHDT(HDT hdt, long id, ValueFactory factory) {
		setHdt(hdt);
		setHdtID(id);
		valueFactory = factory;
	}

	/*---------*
	 * Methods *
	 *---------*/

	protected void setHdt(HDT hdt) {
		Objects.requireNonNull(hdt, "Literal label cannot be null");
		this.hdt = hdt;
	}

	protected void setHdtID(long id) {
		this.hdtID = id;
	}

	protected void parseDatatype() {
		if (!hdt.getDictionary().supportsDataTypeOfId()) {
			parseLiteral();
			return;
		}
		if (datatype == null) {
			CharSequence datatype = hdt.getDictionary().dataTypeOfId(hdtID);
			if (datatype.isEmpty() || datatype.equals(LiteralsUtils.NO_DATATYPE)) {
				this.datatype = (coreDatatype = CoreDatatype.XSD.STRING).getIri();
			} else if (datatype == LiteralsUtils.LITERAL_LANG_TYPE) {
				this.datatype = (coreDatatype = CoreDatatype.RDF.LANGSTRING).getIri();
			} else {
				this.datatype = NTriplesUtil.parseURI(datatype.toString(), valueFactory);
			}
		}
	}

	protected void parseLanguage() {
		if (!hdt.getDictionary().supportsLanguageOfId()) {
			parseLiteral();
			return;
		}
		if (language == null) {
			language = Optional.ofNullable(hdt.getDictionary().languageOfId(hdtID)).map(CharSequence::toString);
		}
	}

	static int indexOf(CharSequence seq, CharSequence s, int start) {
		int n = seq.length() - s.length() + 1;
		loop:
		for (int i = start; i < n; i++) {
			for (int j = 0; j < s.length(); j++) {
				if (seq.charAt(i + j) != s.charAt(j)) {
					continue loop;
				}
			}
			return i;
		}
		return -1;
	}

	static int indexOf(CharSequence seq, char c, int start) {
		for (int i = start; i < seq.length(); i++) {
			if (seq.charAt(i) == c) {
				return i;
			}
		}
		return -1;
	}

	protected void parseLiteral() {
		if (label == null) {
			ByteString literal = ByteString.of(hdt.getDictionary().idToString(hdtID, TripleComponentRole.OBJECT));
			label = (ByteString) LiteralsUtils.removeQuotesTypeAndLang(literal);
			Optional<CharSequence> lang = LiteralsUtils.getLanguage(literal);

			if (lang.isPresent()) {
				language = Optional.of(lang.get().toString());
				datatype = CoreDatatype.RDF.LANGSTRING.getIri();
			} else {
				CharSequence dt = LiteralsUtils.getType(literal);
				if (dt != LiteralsUtils.NO_DATATYPE) {
					datatype = parseURI(dt.toString(), valueFactory);
				} else {
					datatype = XSD.STRING;
				}
				language = Optional.empty();
			}
		}
	}

	@Override
	public String getLabel() {
		parseLiteral();
		if (labelStr == null) {
			labelStr = label.toString();
		}
		return labelStr;
	}

	@Override
	public Optional<String> getLanguage() {
		parseLanguage();
		return language;
	}

	@Override
	public IRI getDatatype() {
		parseDatatype();
		return datatype;
	}

	// Overrides Object.equals(Object), implements Literal.equals(Object)
	@Override
	public boolean equals(Object o) {
		// TODO: This can be probably done more efficiently
		if (this == o) {
			return true;
		}

		if (o instanceof HDTValue hv) {
			return hv.getHDTId() == getHDTId();
		} else if (o instanceof Literal other) {
			// Compare datatypes
			if (!getDatatype().equals(other.getDatatype())) {
				return false;
			}

			// Compare labels
			if (!getLabel().equals(other.getLabel())) {
				return false;
			}

			return getLanguage().equals(other.getLanguage());
		}

		return false;
	}

	// overrides Object.hashCode(), implements Literal.hashCode()
	@Override
	public int hashCode() {
		return getLabel().hashCode();
	}

	/**
	 * Returns the label of the literal with its language or datatype. Note that
	 * this method does not escape the quoted label.
	 *
	 * @see org.eclipse.rdf4j.rio.helpers.NTriplesUtil#toNTriplesString(Literal)
	 */
	@Override
	public String toString() {
		final String label = '"' + getLabel() + '"';

		return getLanguage()

				.map(language -> label + '@' + language)

				.orElseGet(() -> {

					final CoreDatatype datatype = getCoreDatatype();

					return datatype.equals(CoreDatatype.XSD.STRING) ? label
							: label + "^^<" + getDatatype().stringValue() + ">";

				});
	}

	@Override
	public long getHDTId() {
		return hdtID;
	}

	@Override
	public int getHDTPosition() {
		return SimpleIRIHDT.OBJECT_POS; // a literal is only an object
	}

	@Override
	public String stringValue() {
		return getLabel();
	}

	@Override
	public boolean booleanValue() {
		return XMLDatatypeUtil.parseBoolean(getLabel());
	}

	@Override
	public byte byteValue() {
		return (byte) longValue();
	}

	@Override
	public short shortValue() {
		return (short) longValue();
	}

	@Override
	public int intValue() {
		return (int) longValue();
	}

	@Override
	public float floatValue() {
		return (float) doubleValue();
	}

	@Override
	public long longValue() {
		parseLiteral();
		return ByteStringUtil.getLong(label);
	}

	@Override
	public double doubleValue() {
		parseLiteral();
		return ByteStringUtil.getDouble(label);
	}

	@Override
	public BigDecimal decimalValue() {
		parseLiteral();
		return ByteStringUtil.getDecimal(label);
	}

	@Override
	public BigInteger integerValue() {
		return XMLDatatypeUtil.parseInteger(getLabel());
	}

	@Override
	public XMLGregorianCalendar calendarValue() {
		return XMLDatatypeUtil.parseCalendar(getLabel());
	}

	@Override
	public CoreDatatype getCoreDatatype() {
		if (coreDatatype == null) {
			coreDatatype = CoreDatatype.from(getDatatype());
		}
		return coreDatatype;
	}

	@Override
	public void setDelegate(boolean delegate) {
		// ignored
	}

	@Override
	public boolean isDelegate() {
		return true;
	}
}

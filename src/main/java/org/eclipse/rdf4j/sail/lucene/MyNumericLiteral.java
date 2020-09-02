package org.eclipse.rdf4j.sail.lucene;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.SimpleLiteral;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

public class MyNumericLiteral extends SimpleLiteral {
    private static final long serialVersionUID = 3004497457768807919L;
    private final Number number;

    public MyNumericLiteral(Number number, IRI datatype) {
        super(XMLDatatypeUtil.toString(number), datatype);
        this.number = number;
    }

    protected MyNumericLiteral(byte number) {
        this(number, XMLSchema.BYTE);
    }

    protected MyNumericLiteral(short number) {
        this(number, XMLSchema.SHORT);
    }

    protected MyNumericLiteral(int number) {
        this(number, XMLSchema.INT);
    }

    protected MyNumericLiteral(long n) {
        this(n, XMLSchema.LONG);
    }

    protected MyNumericLiteral(float n) {
        this(n, XMLSchema.FLOAT);
    }

    protected MyNumericLiteral(double n) {
        this(n, XMLSchema.DOUBLE);
    }

    public byte byteValue() {
        return this.number.byteValue();
    }

    public short shortValue() {
        return this.number.shortValue();
    }

    public int intValue() {
        return this.number.intValue();
    }

    public long longValue() {
        return this.number.longValue();
    }

    public float floatValue() {
        return this.number.floatValue();
    }

    public double doubleValue() {
        return this.number.doubleValue();
    }
}

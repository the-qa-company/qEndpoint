package com.the_qa_company.qendpoint.core.compact.bitmap;

public interface ModifiableMultiLayerBitmap extends MultiLayerBitmap {
    /**
     * Set the value of the bit at position pos
     *
     * @param layer layer
     * @param position pos
     * @param value    value
     */
    void set(long layer, long position, boolean value);
}

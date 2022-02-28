package com.the_qa_company.q_endpoint.hybridstore;

import org.rdfhdt.hdt.hdt.HDTVersion;

/**
 * store the files used by the hybrid store
 */
public class HybridStoreFiles {

    /**
     * get the file the hdt
     * @param locationHdt the HDT location dir
     * @param hdtIndexName the HDT index name
     * @return the hdt file
     */
    public static String getHDTIndex(String locationHdt, String hdtIndexName) {
        return locationHdt + hdtIndexName;
    }

    // basic locations
    private final String locationNative, locationHdt, hdtIndexName;

    /**
     * create a file getter for a store
     * @param locationNative the native storage dir
     * @param locationHdt the hdt storage dir
     * @param hdtIndexName the hdt index name
     */
    public HybridStoreFiles(String locationNative, String locationHdt, String hdtIndexName) {
        this.locationNative = locationNative;
        this.locationHdt = locationHdt;
        this.hdtIndexName = hdtIndexName;
    }

    /**
     * @return the hdt storage dir
     */
    public String getLocationHdt() {
        return locationHdt;
    }

    /**
     * @return the native storage dir
     */
    public String getLocationNative() {
        return locationNative;
    }

    /**
     * @return the native store A dir
     */
    public String getNativeStoreA() {
        return locationNative + "A";
    }

    /**
     * @return the native store B dir
     */
    public String getNativeStoreB() {
        return locationNative + "B";
    }

    /**
     * @return the native store check file
     */
    public String getWhichStore() {
        return locationNative + "which_store.check";
    }

    /**
     * @return the temp triples file
     */
    public String getTempTriples() {
        return locationNative + "tempTriples.nt";
    }

    /**
     * @return the HDT bit subject (X) file
     */
    public String getHDTBitX() {
        return locationHdt + "bitX";
    }

    /**
     * @return the HDT bit predicate (Y) file
     */
    public String getHDTBitY() {
        return locationHdt + "bitY";
    }

    /**
     * @return the HDT bit objects (Z) file
     */
    public String getHDTBitZ() {
        return locationHdt + "bitZ";
    }

    /**
     * @return the HDT file
     */
    public String getHDTIndex() {
        return getHDTIndex(locationHdt, hdtIndexName);
    }

    /**
     * @return the HDT file with HDT version
     */
    public String getHDTIndexV11() {
        return locationHdt + hdtIndexName + HDTVersion.get_index_suffix("-");
    }

    /**
     * @return the new HDT file
     */
    public String getHDTNewIndex() {
        return locationHdt + hdtIndexName + ".new.hdt";
    }

    /**
     * @return the diff HDT file
     */
    public String getHDTNewIndexDiff() {
        return locationHdt + hdtIndexName + ".diff.new.hdt";
    }

    /**
     * @return the new HDT file with HDT version
     */
    public String getHDTNewIndexV11() {
        return locationHdt + hdtIndexName + ".new.hdt" + HDTVersion.get_index_suffix("-");
    }

    /**
     * @return the delete triple {@link com.the_qa_company.q_endpoint.utils.BitArrayDisk} file
     */
    public String getTripleDeleteArr() {
        return this.locationHdt + "triples-delete.arr";
    }

    /**
     * @return the temp delete triple {@link com.the_qa_company.q_endpoint.utils.BitArrayDisk} file
     */
    public String getTripleDeleteTempArr() {
        return this.locationHdt + "triples-delete-temp.arr";
    }

    /**
     * @return the copy delete triple {@link com.the_qa_company.q_endpoint.utils.BitArrayDisk} file
     */
    public String getTripleDeleteCopyArr() {
        return this.locationHdt + "triples-delete-cpy.arr";
    }

    /**
     * @return the temp RDF delete file
     */
    public String getRDFTempOutput() {
        return locationHdt + "temp.nt";
    }

    /**
     * @return the temp HDT file
     */
    public String getHDTTempOutput() {
        return locationHdt + "temp.hdt";
    }

    /**
     * @return the previous merge file marker
     */
    public String getPreviousMergeFile() {
        return locationHdt + "previous_merge";
    }

}

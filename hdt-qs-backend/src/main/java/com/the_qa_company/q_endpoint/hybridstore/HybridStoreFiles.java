package com.the_qa_company.q_endpoint.hybridstore;

import com.the_qa_company.q_endpoint.utils.sail.builder.ParsedStringValue;
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

    /**
     * the HDT file with HDT version
     * @param locationHdt the HDT location dir
     * @param hdtIndexName the HDT index name
     * @return the hdt file
     */
    public static String getHDTIndexV11(String locationHdt, String hdtIndexName) {
        return locationHdt + hdtIndexName + HDTVersion.get_index_suffix("-");
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
    @ParsedStringValue("locationHDT")
    public String getLocationHdt() {
        return locationHdt;
    }

    /**
     * @return the native storage dir
     */
    @ParsedStringValue("locationNative")
    public String getLocationNative() {
        return locationNative;
    }

    /**
     * @return the native store A dir
     */
    @ParsedStringValue("nativeStore.A")
    public String getNativeStoreA() {
        return locationNative + "A";
    }

    /**
     * @return the native store B dir
     */
    @ParsedStringValue("nativeStore.B")
    public String getNativeStoreB() {
        return locationNative + "B";
    }

    /**
     * @return the native store check file
     */
    @ParsedStringValue("nativeStore.which_store")
    public String getWhichStore() {
        return locationNative + "which_store.check";
    }

    /**
     * @return the temp triples file
     */
    @ParsedStringValue("hybridstore.tempTriples")
    public String getTempTriples() {
        return locationNative + "tempTriples.nt";
    }

    /**
     * @return the HDT bit subject (X) file
     */
    @ParsedStringValue("hybridstore.bitX")
    public String getHDTBitX() {
        return locationHdt + "bitX";
    }

    /**
     * @return the HDT bit predicate (Y) file
     */
    @ParsedStringValue("hybridstore.bitY")
    public String getHDTBitY() {
        return locationHdt + "bitY";
    }

    /**
     * @return the HDT bit objects (Z) file
     */
    @ParsedStringValue("hybridstore.bitZ")
    public String getHDTBitZ() {
        return locationHdt + "bitZ";
    }

    /**
     * @return the HDT file
     */
    @ParsedStringValue("hdt.location")
    public String getHDTIndex() {
        return getHDTIndex(locationHdt, hdtIndexName);
    }

    /**
     * @return the HDT file with HDT version
     */
    @ParsedStringValue("hdt.indexLocation")
    public String getHDTIndexV11() {
        return getHDTIndexV11(locationHdt, hdtIndexName);
    }

    /**
     * @return the new HDT file
     */
    @ParsedStringValue("hdt.new.location")
    public String getHDTNewIndex() {
        return locationHdt + hdtIndexName + ".new.hdt";
    }

    /**
     * @return the diff HDT file
     */
    @ParsedStringValue("hdt.new.diffLocation")
    public String getHDTNewIndexDiff() {
        return locationHdt + hdtIndexName + ".diff.new.hdt";
    }

    /**
     * @return the new HDT file with HDT version
     */
    @ParsedStringValue("hdt.new.diffIndexLocation")
    public String getHDTNewIndexV11() {
        return locationHdt + hdtIndexName + ".new.hdt" + HDTVersion.get_index_suffix("-");
    }

    /**
     * @return the delete triple {@link com.the_qa_company.q_endpoint.utils.BitArrayDisk} file
     */
    @ParsedStringValue("hybridstore.deleteBitmap")
    public String getTripleDeleteArr() {
        return this.locationHdt + "triples-delete.arr";
    }

    /**
     * @return the temp delete triple {@link com.the_qa_company.q_endpoint.utils.BitArrayDisk} file
     */
    @ParsedStringValue("hybridstore.tempDeleteBitmap")
    public String getTripleDeleteTempArr() {
        return this.locationHdt + "triples-delete-temp.arr";
    }

    /**
     * @return the copy delete triple {@link com.the_qa_company.q_endpoint.utils.BitArrayDisk} file
     */
    @ParsedStringValue("hybridstore.tempDeleteBitmapCopy")
    public String getTripleDeleteCopyArr() {
        return this.locationHdt + "triples-delete-cpy.arr";
    }

    /**
     * @return the temp RDF delete file
     */
    @ParsedStringValue("hybridstore.tempRDFOutput")
    public String getRDFTempOutput() {
        return locationHdt + "temp.nt";
    }

    /**
     * @return the temp HDT file
     */
    @ParsedStringValue("hybridstore.tempHDTOutput")
    public String getHDTTempOutput() {
        return locationHdt + "temp.hdt";
    }

    /**
     * @return the previous merge file marker
     */
    @ParsedStringValue("hybridstore.previousMerge")
    public String getPreviousMergeFile() {
        return locationHdt + "previous_merge";
    }

}

package com.the_qa_company.q_endpoint.hybridstore;

public class HybridStoreFiles {
    private final String locationNative, locationHdt, hdtIndexName;
    public static String getHDTIndex(String locationHdt, String hdtIndexName) {
        return locationHdt + hdtIndexName;
    }
    public HybridStoreFiles(String locationNative, String locationHdt, String hdtIndexName) {
        this.locationNative = locationNative;
        this.locationHdt = locationHdt;
        this.hdtIndexName = hdtIndexName;
    }

    public String getLocationHdt() {
        return locationHdt;
    }

    public String getLocationNative() {
        return locationNative;
    }

    public String getNativeStoreA() {
        return locationNative + "A";
    }

    public String getNativeStoreB() {
        return locationNative + "B";
    }

    public String getWhichStore() {
        return locationNative + "which_store.check";
    }

    public String getTempTriples() {
        return locationNative + "tempTriples.nt";
    }

    public String getHDTBitX() {
        return locationHdt + "bitX";
    }

    public String getHDTBitY() {
        return locationHdt + "bitY";
    }

    public String getHDTBitZ() {
        return locationHdt + "bitZ";
    }

    public String getHDTIndex() {
        return getHDTIndex(locationHdt, hdtIndexName);
    }

    public String getHDTIndexV11() {
        return locationHdt + hdtIndexName + ".index.v1-1";
    }

    public String getHDTNewIndex() {
        return locationHdt + hdtIndexName + ".new.hdt";
    }

    public String getHDTNewIndexDiff() {
        return locationHdt + hdtIndexName + ".new.hdt";
    }

    public String getHDTNewIndexV11() {
        return locationHdt + hdtIndexName + "new.hdt.index.v1-1";
    }

    public String getTripleDeleteArr() {
        return this.locationHdt + "triples-delete.arr";
    }

    public String getTripleDeleteTempArr() {
        return this.locationHdt + "triples-delete-temp.arr";
    }

    public String getTripleDeleteNewArr() {
        return this.locationHdt + "triples-delete-new.arr";
    }

    public String getTripleDeleteCopyArr() {
        return this.locationHdt + "triples-delete-cpy.arr";
    }

    public String getRDFTempOutput() {
        return locationHdt + "temp.nt";
    }

    public String getHDTTempOutput() {
        return locationHdt + "temp.hdt";
    }

    public String getPreviousMergeFile() {
        return locationHdt + "previous_merge";
    }

}

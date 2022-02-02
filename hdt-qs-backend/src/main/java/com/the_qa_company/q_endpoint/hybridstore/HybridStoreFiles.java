package com.the_qa_company.q_endpoint.hybridstore;

public class HybridStoreFiles {
    private final String locationNative, locationHdt;

    public HybridStoreFiles(String locationNative, String locationHdt) {
        this.locationNative = locationNative;
        this.locationHdt = locationHdt;
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
        return locationHdt + "index.hdt";
    }

    public String getHDTIndexV11() {
        return locationHdt + "index.hdt.index.v1-1";
    }

    public String getHDTNewIndex() {
        return locationHdt + "new_index.hdt";
    }

    public String getHDTNewIndexDiff() {
        return locationHdt + "new_index_diff.hdt";
    }

    public String getHDTNewIndexV11() {
        return locationHdt + "new_index.hdt.index.v1-1";
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

    public String getStep3RenameMarker() {
        return locationHdt + "rename_merge";
    }

}

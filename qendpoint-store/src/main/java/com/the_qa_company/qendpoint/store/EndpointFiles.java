package com.the_qa_company.qendpoint.store;

import com.the_qa_company.qendpoint.compiler.ParsedStringValue;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;

import java.io.File;
import java.nio.file.Path;

/**
 * store the files used by the endpoint store
 */
public class EndpointFiles {

	/**
	 * get the file the hdt
	 *
	 * @param locationHdt  the HDT location dir
	 * @param hdtIndexName the HDT index name
	 * @return the hdt file
	 */
	public static String getHDTIndex(String locationHdt, String hdtIndexName) {
		return locationHdt + hdtIndexName;
	}

	/**
	 * the HDT file with HDT version
	 *
	 * @param locationHdt  the HDT location dir
	 * @param hdtIndexName the HDT index name
	 * @return the hdt file
	 */
	public static String getHDTIndexV11(String locationHdt, String hdtIndexName) {
		return locationHdt + hdtIndexName + HDTVersion.get_index_suffix("-");
	}

	// basic locations
	private final String locationNative, locationHdt, hdtIndexName;
	private final Path locationNativePath, locationHdtPath;

	/**
	 * create a file getter for a store
	 *
	 * @param locationNative the native storage dir
	 * @param locationHdt    the hdt storage dir
	 * @param hdtIndexName   the hdt index name
	 */
	public EndpointFiles(String locationNative, String locationHdt, String hdtIndexName) {
		this(Path.of(locationNative), Path.of(locationHdt), hdtIndexName);
	}

	/**
	 * create a file getter for a store
	 *
	 * @param locationNative the native storage dir
	 * @param locationHdt    the hdt storage dir
	 * @param hdtIndexName   the hdt index name
	 */
	public EndpointFiles(Path locationNative, Path locationHdt, String hdtIndexName) {
		this.locationNative = locationNative.toAbsolutePath() + File.separator;
		this.locationHdt = locationHdt.toAbsolutePath() + File.separator;
		this.hdtIndexName = hdtIndexName;
		this.locationNativePath = locationNative;
		this.locationHdtPath = locationHdt;
	}

	/**
	 * create a file getter for a store
	 *
	 * @param locationEndpoint the qendpoint location dir
	 * @param hdtIndexName     the hdt index name
	 */
	public EndpointFiles(Path locationEndpoint, String hdtIndexName) {
		this(locationEndpoint.resolve("native-store"), locationEndpoint.resolve("hdt-store"), hdtIndexName);
	}

	/**
	 * create a file getter for a store
	 *
	 * @param locationEndpoint the qendpoint location dir
	 */
	public EndpointFiles(Path locationEndpoint) {
		this(locationEndpoint, "index_dev.hdt");
	}

	/**
	 * @return path of {@link #getLocationHdt()}
	 */
	public Path getLocationHdtPath() {
		return locationHdtPath;
	}

	/**
	 * @return path of {@link #getLocationNative()}
	 */
	public Path getLocationNativePath() {
		return locationNativePath;
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
	@ParsedStringValue("store.tempTriples")
	public String getTempTriples() {
		return locationNative + "tempTriples.nt";
	}

	/**
	 * @return the HDT bit subject (X) file
	 */
	@ParsedStringValue("store.bitX")
	public String getHDTBitX() {
		return locationHdt + "bitX";
	}

	/**
	 * @return the HDT bit predicate (Y) file
	 */
	@ParsedStringValue("store.bitY")
	public String getHDTBitY() {
		return locationHdt + "bitY";
	}

	/**
	 * @return the HDT bit objects (Z) file
	 */
	@ParsedStringValue("store.bitZ")
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
	 * @return the HDT file
	 */
	public Path getHDTIndexPath() {
		return Path.of(getHDTIndex());
	}

	/**
	 * @return the HDT file with HDT version
	 */
	public Path getHDTIndexV11Path() {
		return Path.of(getHDTIndexV11());
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
	 * @return the delete triple
	 *         {@link com.the_qa_company.qendpoint.utils.BitArrayDisk} file
	 */
	@ParsedStringValue("store.deleteBitmap")
	public String getTripleDeleteArr() {
		return this.locationHdt + "triples-delete.arr";
	}

	/**
	 * @return the temp delete triple
	 *         {@link com.the_qa_company.qendpoint.utils.BitArrayDisk} file
	 */
	@ParsedStringValue("store.tempDeleteBitmap")
	public String getTripleDeleteTempArr() {
		return this.locationHdt + "triples-delete-temp.arr";
	}

	/**
	 * @return the copy delete triple
	 *         {@link com.the_qa_company.qendpoint.utils.BitArrayDisk} file
	 */
	@ParsedStringValue("store.tempDeleteBitmapCopy")
	public String getTripleDeleteCopyArr() {
		return this.locationHdt + "triples-delete-cpy.arr";
	}

	/**
	 * @return the temp RDF delete file
	 */
	@ParsedStringValue("store.tempRDFOutput")
	public String getRDFTempOutput() {
		return locationHdt + "temp.nt";
	}

	/**
	 * @return the temp HDT file
	 */
	@ParsedStringValue("store.tempHDTOutput")
	public String getHDTTempOutput() {
		return locationHdt + "temp.hdt";
	}

	/**
	 * @return the previous merge file marker
	 */
	@ParsedStringValue("store.previousMerge")
	public String getPreviousMergeFile() {
		return locationHdt + "previous_merge";
	}

}

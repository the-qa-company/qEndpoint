package com.the_qa_company.qendpoint.core.hdt;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.BitmapFactory;
import com.the_qa_company.qendpoint.core.enums.CompressionType;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.rdf.TripleWriter;
import com.the_qa_company.qendpoint.core.triples.TripleString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public abstract class HDTManager {

	private static HDTManager instance;

	private static HDTManager getInstance() {
		if (instance == null) {
			try {
				// Try to instantiate pro
				Class<?> managerImplClass = Class.forName("com.the_qa_company.qendpoint.core.pro.HDTManagerProImpl");
				instance = (HDTManager) managerImplClass.getDeclaredConstructor().newInstance();
			} catch (Exception e1) {
				try {
					// Pro not found, instantiate normal
					Class<?> managerImplClass = Class.forName("com.the_qa_company.qendpoint.core.hdt.HDTManagerImpl");
					instance = (HDTManager) managerImplClass.getDeclaredConstructor().newInstance();
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(
							"Class com.the_qa_company.qendpoint.core.hdt.HDTManagerImpl not found. Did you include the HDT implementation jar?",
							e);
				} catch (InstantiationException | InvocationTargetException e) {
					throw new RuntimeException(
							"Cannot create implementation for HDTManager. Does the class com.the_qa_company.qendpoint.core.hdt.HDTManagerImpl inherit from HDTManager?",
							e);
				} catch (NoSuchMethodException e) {
					throw new RuntimeException(
							"Cannot create implementation for HDTManager. Does the class com.the_qa_company.qendpoint.core.hdt.HDTManagerImpl has a default empty constructor?",
							e);
				} catch (IllegalAccessException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return instance;
	}

	public static HDTOptions readOptions(Path file) throws IOException {
		return readOptions(file.toAbsolutePath().toString());
	}

	public static HDTOptions readOptions(String file) throws IOException {
		return HDTManager.getInstance().doReadOptions(file);
	}

	/**
	 * Load an HDT file into memory to use it. NOTE: Use this method to go
	 * through all elements. If you plan to do queries, use loadIndexedHDT()
	 * instead.
	 *
	 * @param hdtFileName file path to load
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @param hdtFormat   Parameters to tune the loaded HDT index. Can be null
	 *                    for default options.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadHDT(Path hdtFileName, ProgressListener listener, HDTOptions hdtFormat) throws IOException {
		return loadHDT(hdtFileName.toAbsolutePath().toString(), listener, hdtFormat);
	}

	/**
	 * Load an HDT file into memory to use it. NOTE: Use this method to go
	 * through all elements. If you plan to do queries, use loadIndexedHDT()
	 * instead.
	 *
	 * @param hdtFileName file path to load
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @param hdtFormat   Parameters to tune the loaded HDT index. Can be null
	 *                    for default options.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadHDT(String hdtFileName, ProgressListener listener, HDTOptions hdtFormat) throws IOException {
		return HDTManager.getInstance().doLoadHDT(hdtFileName, ProgressListener.ofNullable(listener),
				HDTOptions.ofNullable(hdtFormat));
	}

	/**
	 * Load an HDT file into memory to use it. NOTE: Use this method to go
	 * through all elements. If you plan to do queries, use loadIndexedHDT()
	 * instead.
	 *
	 * @param hdtFileName file path to load
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadHDT(Path hdtFileName, ProgressListener listener) throws IOException {
		return loadHDT(hdtFileName.toAbsolutePath().toString(), listener);
	}

	/**
	 * Load an HDT file into memory to use it. NOTE: Use this method to go
	 * through all elements. If you plan to do queries, use loadIndexedHDT()
	 * instead.
	 *
	 * @param hdtFileName file path to load
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadHDT(String hdtFileName, ProgressListener listener) throws IOException {
		return loadHDT(hdtFileName, listener, null);
	}

	/**
	 * Load an HDT file into memory to use it. NOTE: Use this method to go
	 * through all elements. If you plan to do queries, use loadIndexedHDT()
	 * instead.
	 *
	 * @param hdtFileName file path to load
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadHDT(Path hdtFileName) throws IOException {
		return loadHDT(hdtFileName.toAbsolutePath().toString());
	}

	/**
	 * Load an HDT file into memory to use it. NOTE: Use this method to go
	 * through all elements. If you plan to do queries, use loadIndexedHDT()
	 * instead.
	 *
	 * @param hdtFileName file path to load
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadHDT(String hdtFileName) throws IOException {
		return loadHDT(hdtFileName, null, null);
	}

	/**
	 * Map an HDT file into memory to use it. This method does not load the
	 * whole file into memory, it lets the OS to handle memory pages as desired.
	 * Therefore it uses less memory but can be slower for querying because it
	 * needs to load those blocks from disk. NOTE: Use this method to go through
	 * all elements. If you plan to do queries, use mapIndexedHDT() instead.
	 *
	 * @param hdtFileName file path to map
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @param hdtFormat   Parameters to tune the loaded HDT index. Can be null
	 *                    for default options.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapHDT(Path hdtFileName, ProgressListener listener, HDTOptions hdtFormat) throws IOException {
		return mapHDT(hdtFileName.toAbsolutePath().toString(), listener, hdtFormat);
	}

	/**
	 * Map an HDT file into memory to use it. This method does not load the
	 * whole file into memory, it lets the OS to handle memory pages as desired.
	 * Therefore it uses less memory but can be slower for querying because it
	 * needs to load those blocks from disk. NOTE: Use this method to go through
	 * all elements. If you plan to do queries, use mapIndexedHDT() instead.
	 *
	 * @param hdtFileName file path to map
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @param hdtFormat   Parameters to tune the loaded HDT index. Can be null
	 *                    for default options.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapHDT(String hdtFileName, ProgressListener listener, HDTOptions hdtFormat) throws IOException {
		return HDTManager.getInstance().doMapHDT(hdtFileName, ProgressListener.ofNullable(listener),
				HDTOptions.ofNullable(hdtFormat));
	}

	/**
	 * Map an HDT file into memory to use it. This method does not load the
	 * whole file into memory, it lets the OS to handle memory pages as desired.
	 * Therefore it uses less memory but can be slower for querying because it
	 * needs to load those blocks from disk. NOTE: Use this method to go through
	 * all elements. If you plan to do queries, use mapIndexedHDT() instead.
	 *
	 * @param hdtFileName file path to map
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapHDT(Path hdtFileName, ProgressListener listener) throws IOException {
		return mapHDT(hdtFileName.toAbsolutePath().toString(), listener);
	}

	/**
	 * Map an HDT file into memory to use it. This method does not load the
	 * whole file into memory, it lets the OS to handle memory pages as desired.
	 * Therefore it uses less memory but can be slower for querying because it
	 * needs to load those blocks from disk. NOTE: Use this method to go through
	 * all elements. If you plan to do queries, use mapIndexedHDT() instead.
	 *
	 * @param hdtFileName file path to map
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapHDT(String hdtFileName, ProgressListener listener) throws IOException {
		return mapHDT(hdtFileName, listener, null);
	}

	/**
	 * Map an HDT file into memory to use it. This method does not load the
	 * whole file into memory, it lets the OS to handle memory pages as desired.
	 * Therefore it uses less memory but can be slower for querying because it
	 * needs to load those blocks from disk. NOTE: Use this method to go through
	 * all elements. If you plan to do queries, use mapIndexedHDT() instead.
	 *
	 * @param hdtFileName file path to map
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapHDT(Path hdtFileName) throws IOException {
		return mapHDT(hdtFileName.toAbsolutePath().toString());
	}

	/**
	 * Map an HDT file into memory to use it. This method does not load the
	 * whole file into memory, it lets the OS to handle memory pages as desired.
	 * Therefore it uses less memory but can be slower for querying because it
	 * needs to load those blocks from disk. NOTE: Use this method to go through
	 * all elements. If you plan to do queries, use mapIndexedHDT() instead.
	 *
	 * @param hdtFileName file path to map
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapHDT(String hdtFileName) throws IOException {
		return mapHDT(hdtFileName, null, null);
	}

	/**
	 * Load an HDT from an InputStream (File, socket...). NOTE: Use this method
	 * to go through all elements. If you plan to do queries, use
	 * loadIndexedHDT() instead.
	 *
	 * @param hdtFile   file path to load
	 * @param listener  Listener to get notified of loading progress. Can be
	 *                  null if no notifications needed.
	 * @param hdtFormat Parameters to tune the loaded HDT index. Can be null for
	 *                  default options.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadHDT(InputStream hdtFile, ProgressListener listener, HDTOptions hdtFormat) throws IOException {
		return HDTManager.getInstance().doLoadHDT(hdtFile, ProgressListener.ofNullable(listener),
				HDTOptions.ofNullable(hdtFormat));
	}

	/**
	 * Load an HDT from an InputStream (File, socket...). NOTE: Use this method
	 * to go through all elements. If you plan to do queries, use
	 * loadIndexedHDT() instead.
	 *
	 * @param hdtFile  file path to load
	 * @param listener Listener to get notified of loading progress. Can be null
	 *                 if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadHDT(InputStream hdtFile, ProgressListener listener) throws IOException {
		return loadHDT(hdtFile, listener, null);
	}

	/**
	 * Load an HDT from an InputStream (File, socket...). NOTE: Use this method
	 * to go through all elements. If you plan to do queries, use
	 * loadIndexedHDT() instead.
	 *
	 * @param hdtFile file path to load
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadHDT(InputStream hdtFile) throws IOException {
		return loadHDT(hdtFile, null, null);
	}

	/**
	 * Load an HDT File, and load/create additional indexes to support all kind
	 * of queries efficiently.
	 *
	 * @param hdtFileName file path to load
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @param hdtFormat   Parameters to tune the loaded HDT index. Can be null
	 *                    for default options.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadIndexedHDT(Path hdtFileName, ProgressListener listener, HDTOptions hdtFormat)
			throws IOException {
		return loadIndexedHDT(hdtFileName.toAbsolutePath().toString(), listener, hdtFormat);
	}

	/**
	 * Load an HDT File, and load/create additional indexes to support all kind
	 * of queries efficiently.
	 *
	 * @param hdtFileName file path to load
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadIndexedHDT(Path hdtFileName, ProgressListener listener) throws IOException {
		return loadIndexedHDT(hdtFileName.toAbsolutePath().toString(), listener);
	}

	/**
	 * Load an HDT File, and load/create additional indexes to support all kind
	 * of queries efficiently.
	 *
	 * @param hdtFileName file path to load
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadIndexedHDT(Path hdtFileName) throws IOException {
		return loadIndexedHDT(hdtFileName.toAbsolutePath().toString());
	}

	/**
	 * Load an HDT File, and load/create additional indexes to support all kind
	 * of queries efficiently.
	 *
	 * @param hdtFileName file path to load
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @param hdtFormat   Parameters to tune the loaded HDT index. Can be null
	 *                    for default options.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadIndexedHDT(String hdtFileName, ProgressListener listener, HDTOptions hdtFormat)
			throws IOException {
		return HDTManager.getInstance().doLoadIndexedHDT(hdtFileName, ProgressListener.ofNullable(listener),
				HDTOptions.ofNullable(hdtFormat));
	}

	/**
	 * Load an HDT File, and load/create additional indexes to support all kind
	 * of queries efficiently.
	 *
	 * @param hdtFileName file path to load
	 * @param listener    – Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadIndexedHDT(String hdtFileName, ProgressListener listener) throws IOException {
		return loadIndexedHDT(hdtFileName, listener, null);
	}

	/**
	 * Load an HDT File, and load/create additional indexes to support all kind
	 * of queries efficiently.
	 *
	 * @param hdtFileName file path to load
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadIndexedHDT(String hdtFileName) throws IOException {
		return loadIndexedHDT(hdtFileName, null, null);
	}

	/**
	 * Maps an HDT File into virtual memory, and load/create additional indexes
	 * to support all kind of queries efficiently.
	 *
	 * @param hdtFileName file path to map
	 * @param spec        HDTOptions to the new mapped HDT. Can be null for
	 *                    default options.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapIndexedHDT(Path hdtFileName, HDTOptions spec, ProgressListener listener) throws IOException {
		return mapIndexedHDT(hdtFileName.toAbsolutePath().toString(), spec, listener);
	}

	/**
	 * Maps an HDT File into virtual memory, and load/create additional indexes
	 * to support all kind of queries efficiently.
	 *
	 * @param hdtFileName file path to map
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapIndexedHDT(Path hdtFileName, ProgressListener listener) throws IOException {
		return mapIndexedHDT(hdtFileName.toAbsolutePath().toString(), ProgressListener.ofNullable(listener));
	}

	/**
	 * Maps an HDT File into virtual memory, and load/create additional indexes
	 * to support all kind of queries efficiently.
	 *
	 * @param hdtFileName file path to map
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapIndexedHDT(Path hdtFileName) throws IOException {
		return mapIndexedHDT(hdtFileName.toAbsolutePath().toString());
	}

	/**
	 * Maps an HDT File into virtual memory, and load/create additional indexes
	 * to support all kind of queries efficiently.
	 *
	 * @param hdtFileName file path to map
	 * @param spec        HDTOptions to the new mapped HDT. Can be null for
	 *                    default options.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapIndexedHDT(String hdtFileName, HDTOptions spec, ProgressListener listener) throws IOException {
		return HDTManager.getInstance().doMapIndexedHDT(hdtFileName, ProgressListener.ofNullable(listener),
				HDTOptions.ofNullable(spec));
	}

	/**
	 * Maps an HDT File into virtual memory, and load/create additional indexes
	 * to support all kind of queries efficiently.
	 *
	 * @param hdtFileName file path to map
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapIndexedHDT(String hdtFileName, ProgressListener listener) throws IOException {
		return mapIndexedHDT(hdtFileName, null, listener);
	}

	/**
	 * Maps an HDT File into virtual memory, and load/create additional indexes
	 * to support all kind of queries efficiently.
	 *
	 * @param hdtFileName file path to map
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT mapIndexedHDT(String hdtFileName) throws IOException {
		return mapIndexedHDT(hdtFileName, null, null);
	}

	/**
	 * Load an HDT file from InputStream, and create additional indexes to
	 * support all kind of queries efficiently.
	 *
	 * @param hdtFileName file path to load
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadIndexedHDT(InputStream hdtFileName, ProgressListener listener) throws IOException {
		return loadIndexedHDT(hdtFileName, listener, null);
	}

	/**
	 * Load an HDT file from InputStream, and create additional indexes to
	 * support all kind of queries efficiently.
	 *
	 * @param hdtFileName file path to load
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadIndexedHDT(InputStream hdtFileName) throws IOException {
		return loadIndexedHDT(hdtFileName, null, null);
	}

	/**
	 * Load an HDT file from InputStream, and create additional indexes to
	 * support all kind of queries efficiently.
	 *
	 * @param hdtFileName file path to load
	 * @param hdtFormat   Parameters to tune the loaded HDT. Can be null for
	 *                    default options.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT loadIndexedHDT(InputStream hdtFileName, ProgressListener listener, HDTOptions hdtFormat)
			throws IOException {
		return HDTManager.getInstance().doLoadIndexedHDT(hdtFileName, ProgressListener.ofNullable(listener),
				HDTOptions.ofNullable(hdtFormat));
	}

	/**
	 * Return an indexed HDT that is efficient for all kind of queries, given a
	 * not indexed HDT.
	 *
	 * @param hdt      file path to index
	 * @param listener Listener to get notified of loading progress. Can be null
	 *                 if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT indexedHDT(HDT hdt, ProgressListener listener) throws IOException {
		return HDTManager.getInstance().doIndexedHDT(hdt, ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF file.
	 *
	 * @param rdfFileName File name.
	 * @param baseURI     Base URI for the dataset.
	 * @param rdfNotation Format of the source RDF File (NTriples, N3,
	 *                    RDF-XML...)
	 * @param hdtFormat   Parameters to tune the generated HDT.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the file cannot be parsed
	 */
	public static HDT generateHDT(Path rdfFileName, String baseURI, RDFNotation rdfNotation, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException {
		return generateHDT(rdfFileName.toAbsolutePath().toString(), baseURI, rdfNotation, hdtFormat, listener);
	}

	/**
	 * Create an HDT file from an RDF file.
	 *
	 * @param rdfFileName File name.
	 * @param baseURI     Base URI for the dataset.
	 * @param rdfNotation Format of the source RDF File (NTriples, N3,
	 *                    RDF-XML...)
	 * @param hdtFormat   Parameters to tune the generated HDT.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the file cannot be parsed
	 */
	public static HDT generateHDT(String rdfFileName, String baseURI, RDFNotation rdfNotation, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDT(rdfFileName, baseURI, rdfNotation,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF file.
	 *
	 * @param iterator  A provider of triples. Must implement hasNext(), next()
	 *                  and estimatedNumResults.
	 * @param baseURI   Base URI for the dataset.
	 * @param hdtFormat Parameters to tune the generated HDT.
	 * @param listener  Listener to get notified of loading progress. Can be
	 *                  null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the file cannot be parsed
	 */
	public static HDT generateHDT(Iterator<TripleString> iterator, String baseURI, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDT(iterator, baseURI, HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from a RDF stream.
	 *
	 * @param fileStream RDF stream to parse.
	 * @param baseURI    Base URI for the dataset.
	 * @param filename   the RDF file name to guess the stream format and
	 *                   compresion.
	 * @param hdtFormat  Parameters to tune the generated HDT.
	 * @param listener   Listener to get notified of loading progress. Can be
	 *                   null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the stream cannot be used
	 * @throws ParserException when the RDF stream can't be parsed
	 */
	public static HDT generateHDT(InputStream fileStream, String baseURI, String filename, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDT(fileStream, baseURI, RDFNotation.guess(filename),
				CompressionType.guess(filename), HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from a RDF stream.
	 *
	 * @param fileStream      RDF stream to parse.
	 * @param baseURI         Base URI for the dataset.
	 * @param rdfNotation     Format of the source RDF stream (NTriples, N3,
	 *                        RDF-XML...)
	 * @param compressionType Compression type of the RDF stream. (GZIP, ZIP...)
	 * @param hdtFormat       Parameters to tune the generated HDT.
	 * @param listener        Listener to get notified of loading progress. Can
	 *                        be null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the stream cannot be used
	 * @throws ParserException when the RDF stream can't be parsed
	 */
	public static HDT generateHDT(InputStream fileStream, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDT(fileStream, baseURI, rdfNotation, compressionType,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from a RDF stream.
	 *
	 * @param fileStream  RDF stream to parse.
	 * @param baseURI     Base URI for the dataset.
	 * @param rdfNotation Format of the source RDF stream (NTriples, N3,
	 *                    RDF-XML...)
	 * @param hdtFormat   Parameters to tune the generated HDT.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the stream cannot be used
	 * @throws ParserException when the RDF stream can't be parsed
	 */
	public static HDT generateHDT(InputStream fileStream, String baseURI, RDFNotation rdfNotation, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDT(fileStream, baseURI, rdfNotation, CompressionType.NONE,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF file by sorting the triples on disk,
	 * reduce the memory required by increasing the IO usage.
	 *
	 * @param rdfFileName     RDF file to parse.
	 * @param baseURI         Base URI for the dataset.
	 * @param rdfNotation     Format of the source RDF File (NTriples, N3,
	 *                        RDF-XML...)
	 * @param compressionType Compression type of the RDF file. (GZIP, ZIP...)
	 * @param hdtFormat       Parameters to tune the generated HDT.
	 * @param listener        Listener to get notified of loading progress. Can
	 *                        be null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the RDF file can't be parsed
	 */
	public static HDT generateHDTDisk(String rdfFileName, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDTDisk(rdfFileName, baseURI, rdfNotation, compressionType,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF file without compression by sorting the
	 * triples on disk, reduce the memory required by increasing the IO usage.
	 *
	 * @param rdfFileName RDF file to parse.
	 * @param baseURI     Base URI for the dataset.
	 * @param rdfNotation Format of the source RDF File (NTriples, N3,
	 *                    RDF-XML...)
	 * @param hdtFormat   Parameters to tune the generated HDT.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the RDF file can't be parsed
	 */
	public static HDT generateHDTDisk(String rdfFileName, String baseURI, RDFNotation rdfNotation, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDTDisk(rdfFileName, baseURI, rdfNotation, CompressionType.NONE,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF file by sorting the triples on disk,
	 * reduce the memory required by increasing the IO usage. Will guess the RDF
	 * file compression/format with the file name.
	 *
	 * @param rdfFileName RDF file to parse.
	 * @param baseURI     Base URI for the dataset.
	 * @param hdtFormat   Parameters to tune the generated HDT.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the RDF file can't be parsed
	 */
	public static HDT generateHDTDisk(String rdfFileName, String baseURI, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDTDisk(rdfFileName, baseURI, RDFNotation.guess(rdfFileName),
				CompressionType.guess(rdfFileName), HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF stream by sorting the triples on disk,
	 * reduce the memory required by increasing the IO usage.
	 *
	 * @param fileStream RDF stream to parse.
	 * @param baseURI    Base URI for the dataset.
	 * @param filename   the RDF file name to guess the stream format and
	 *                   compresion.
	 * @param hdtFormat  Parameters to tune the generated HDT.
	 * @param listener   Listener to get notified of loading progress. Can be
	 *                   null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the stream cannot be used
	 * @throws ParserException when the RDF stream can't be parsed
	 */
	public static HDT generateHDTDisk(InputStream fileStream, String baseURI, String filename, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDTDisk(fileStream, baseURI, RDFNotation.guess(filename),
				CompressionType.guess(filename), HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF stream by sorting the triples on disk,
	 * reduce the memory required by increasing the IO usage.
	 *
	 * @param fileStream      RDF stream to parse.
	 * @param baseURI         Base URI for the dataset.
	 * @param rdfNotation     Format of the source RDF stream (NTriples, N3,
	 *                        RDF-XML...)
	 * @param compressionType Compression type of the RDF stream. (GZIP, ZIP...)
	 * @param hdtFormat       Parameters to tune the generated HDT.
	 * @param listener        Listener to get notified of loading progress. Can
	 *                        be null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the stream cannot be used
	 * @throws ParserException when the RDF stream can't be parsed
	 */
	public static HDT generateHDTDisk(InputStream fileStream, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDTDisk(fileStream, baseURI, rdfNotation, compressionType,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF stream by sorting the triples on disk,
	 * reduce the memory required by increasing the IO usage.
	 *
	 * @param fileStream  RDF stream to parse.
	 * @param baseURI     Base URI for the dataset.
	 * @param rdfNotation Format of the source RDF stream (NTriples, N3,
	 *                    RDF-XML...)
	 * @param hdtFormat   Parameters to tune the generated HDT.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the stream cannot be used
	 * @throws ParserException when the RDF stream can't be parsed
	 */
	public static HDT generateHDTDisk(InputStream fileStream, String baseURI, RDFNotation rdfNotation,
			HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDTDisk(fileStream, baseURI, rdfNotation, CompressionType.NONE,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF stream by sorting the triples on disk,
	 * reduce the memory required by increasing the IO usage.
	 *
	 * @param baseURI   Base URI for the dataset.
	 * @param hdtFormat Parameters to tune the generated HDT.
	 * @param listener  Listener to get notified of loading progress. Can be
	 *                  null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the stream cannot be used
	 */
	public static HDT generateHDTDisk(Iterator<TripleString> iterator, String baseURI, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doGenerateHDTDisk(iterator, baseURI, HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener));
	}

	public static TripleWriter getHDTWriter(OutputStream out, String baseURI, HDTOptions hdtFormat) throws IOException {
		return HDTManager.getInstance().doGetHDTWriter(out, baseURI, HDTOptions.ofNullable(hdtFormat));
	}

	public static TripleWriter getHDTWriter(Path outFile, String baseURI, HDTOptions hdtFormat) throws IOException {
		return getHDTWriter(outFile.toAbsolutePath().toString(), baseURI, hdtFormat);
	}

	public static TripleWriter getHDTWriter(String outFile, String baseURI, HDTOptions hdtFormat) throws IOException {
		return HDTManager.getInstance().doGetHDTWriter(outFile, baseURI, HDTOptions.ofNullable(hdtFormat));
	}

	/**
	 * Create an HDT file from two HDT files by joining the triples.
	 *
	 * @param location     where the new HDT file is stored
	 * @param hdtFileName1 First hdt file name
	 * @param hdtFileName2 Second hdt file name
	 * @param hdtFormat    Parameters to tune the generated HDT.
	 * @param listener     Listener to get notified of loading progress. Can be
	 *                     null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT catHDT(Path location, Path hdtFileName1, Path hdtFileName2, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		return catHDT(location.toAbsolutePath().toString(), hdtFileName1.toAbsolutePath().toString(),
				hdtFileName2.toAbsolutePath().toString(), hdtFormat, listener);
	}

	/**
	 * Create an HDT file from two HDT files by joining the triples.
	 *
	 * @param location     where the new HDT file is stored
	 * @param hdtFileName1 First hdt file name
	 * @param hdtFileName2 Second hdt file name
	 * @param hdtFormat    Parameters to tune the generated HDT.
	 * @param listener     Listener to get notified of loading progress. Can be
	 *                     null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT catHDT(String location, String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		return HDTManager.getInstance().doHDTCat(location, hdtFileName1, hdtFileName2, HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from HDT files by joining the triples.
	 *
	 * @param hdtFileNames hdt file names
	 * @param hdtFormat    Parameters to tune the generated HDT.
	 * @param listener     Listener to get notified of loading progress. Can be
	 *                     null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT catHDTPath(List<Path> hdtFileNames, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException {
		return catHDT(hdtFileNames.stream().map(p -> p.toAbsolutePath().toString()).collect(Collectors.toList()),
				hdtFormat, listener);
	}

	/**
	 * Create an HDT file from HDT files by joining the triples.
	 *
	 * @param hdtFileNames hdt file names
	 * @param hdtFormat    Parameters to tune the generated HDT.
	 * @param listener     Listener to get notified of loading progress. Can be
	 *                     null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT catHDT(List<String> hdtFileNames, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException {
		return HDTManager.getInstance().doHDTCat(hdtFileNames, HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener));
	}

	/**
	 * Create a new HDT by removing from hdt1 the triples of hdt2.
	 *
	 * @param hdtFileName1 First hdt file name
	 * @param hdtFileName2 Second hdt file name
	 * @param hdtFormat    Parameters to tune the generated HDT.
	 * @param listener     Listener to get notified of loading progress. Can be
	 *                     null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT diffHDT(Path hdtFileName1, Path hdtFileName2, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException {
		return diffHDT(hdtFileName1.toAbsolutePath().toString(), hdtFileName2.toAbsolutePath().toString(), hdtFormat,
				listener);
	}

	/**
	 * Create a new HDT by removing from hdt1 the triples of hdt2.
	 *
	 * @param hdtFileName1 First hdt file name
	 * @param hdtFileName2 Second hdt file name
	 * @param hdtFormat    Parameters to tune the generated HDT.
	 * @param listener     Listener to get notified of loading progress. Can be
	 *                     null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT diffHDT(String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException {
		return HDTManager.getInstance().doHDTDiff(hdtFileName1, hdtFileName2, HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener));
	}

	/**
	 * Create a new HDT by removing from a hdt all the triples marked to delete
	 * in a bitmap
	 *
	 * @param location     where the new HDT file is stored
	 * @param hdtFileName  hdt file name
	 * @param deleteBitmap delete bitmap
	 * @param hdtFormat    Parameters to tune the generated HDT.
	 * @param listener     Listener to get notified of loading progress. Can be
	 *                     null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT diffHDTBit(Path location, String hdtFileName, Bitmap deleteBitmap, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		return diffHDTBit(location.toAbsolutePath().toString(), hdtFileName, deleteBitmap, hdtFormat, listener);
	}

	/**
	 * Create a new HDT by removing from a hdt all the triples marked to delete
	 * in a bitmap
	 *
	 * @param location     where the new HDT file is stored
	 * @param hdtFileName  hdt file name
	 * @param deleteBitmap delete bitmap
	 * @param hdtFormat    Parameters to tune the generated HDT.
	 * @param listener     Listener to get notified of loading progress. Can be
	 *                     null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT diffHDTBit(String location, String hdtFileName, Bitmap deleteBitmap, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		return HDTManager.getInstance().doHDTDiffBit(location, hdtFileName, deleteBitmap,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from HDT files by joining the triples and removing
	 * some triples with delete bitmaps
	 *
	 * @param hdtFileNames  hdt file names
	 * @param deleteBitmaps the bitmaps for each HDT in hdtFileNames, should be
	 *                      the same size as hdtFileNames, see
	 *                      {@link BitmapFactory#empty()}
	 * @param hdtFormat     Parameters to tune the generated HDT.
	 * @param listener      Listener to get notified of loading progress. Can be
	 *                      null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT diffBitCatHDTPath(List<Path> hdtFileNames, List<? extends Bitmap> deleteBitmaps,
			HDTOptions hdtFormat, ProgressListener listener) throws IOException {
		return diffBitCatHDT(hdtFileNames.stream().map(p -> p.toAbsolutePath().toString()).collect(Collectors.toList()),
				deleteBitmaps, hdtFormat, listener);
	}

	/**
	 * Create an HDT file from HDT files by joining the triples and removing
	 * some triples with delete bitmaps
	 *
	 * @param hdtFileNames  hdt file names
	 * @param deleteBitmaps the bitmaps for each HDT in hdtFileNames, should be
	 *                      the same size as hdtFileNames, see
	 *                      {@link BitmapFactory#empty()}
	 * @param hdtFormat     Parameters to tune the generated HDT.
	 * @param listener      Listener to get notified of loading progress. Can be
	 *                      null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT diffBitCatHDT(List<String> hdtFileNames, List<? extends Bitmap> deleteBitmaps,
			HDTOptions hdtFormat, ProgressListener listener) throws IOException {
		return HDTManager.getInstance().doHDTDiffBitCat(hdtFileNames, deleteBitmaps, HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from HDT files by joining the triples and removing
	 * some triples with delete bitmaps
	 *
	 * @param hdts          hdts to merge
	 * @param deleteBitmaps the bitmaps for each HDT in hdtFileNames, should be
	 *                      the same size as hdtFileNames, see
	 *                      {@link BitmapFactory#empty()}
	 * @param hdtFormat     Parameters to tune the generated HDT.
	 * @param listener      Listener to get notified of loading progress. Can be
	 *                      null if no notifications needed.
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT diffBitCatHDTObject(List<HDT> hdts, List<? extends Bitmap> deleteBitmaps, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException {
		return diffBitCatHDTObject(hdts, deleteBitmaps, hdtFormat, listener, false);
	}

	/**
	 * Create an HDT file from HDT files by joining the triples and removing
	 * some triples with delete bitmaps
	 *
	 * @param hdts          hdts to merge
	 * @param deleteBitmaps the bitmaps for each HDT in hdtFileNames, should be
	 *                      the same size as hdtFileNames, see
	 *                      {@link BitmapFactory#empty()}
	 * @param hdtFormat     Parameters to tune the generated HDT.
	 * @param listener      Listener to get notified of loading progress. Can be
	 *                      null if no notifications needed.
	 * @param closeHDTs     close the HDTs after diffcat
	 * @return HDT
	 * @throws IOException when the file cannot be found
	 */
	public static HDT diffBitCatHDTObject(List<HDT> hdts, List<? extends Bitmap> deleteBitmaps, HDTOptions hdtFormat,
			ProgressListener listener, boolean closeHDTs) throws IOException {
		return HDTManager.getInstance().doHDTDiffBitCatObject(hdts, deleteBitmaps, HDTOptions.ofNullable(hdtFormat),
				ProgressListener.ofNullable(listener), closeHDTs);
	}

	/**
	 * Create an HDT file from an RDF file in a tree, stop the chunk creation
	 * with the fluxStop
	 *
	 * @param fluxStop    Flux stopper
	 * @param supplier    HDT supplier to create initial HDT before cat
	 * @param rdfFileName File name.
	 * @param baseURI     Base URI for the dataset.
	 * @param rdfNotation Format of the source RDF File (NTriples, N3,
	 *                    RDF-XML...)
	 * @param hdtFormat   Parameters to tune the generated HDT.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the file cannot be parsed
	 */
	public static HDT catTree(RDFFluxStop fluxStop, HDTSupplier supplier, Path rdfFileName, String baseURI,
			RDFNotation rdfNotation, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		return catTree(fluxStop, supplier, rdfFileName.toAbsolutePath().toString(), baseURI, rdfNotation, hdtFormat,
				listener);
	}

	/**
	 * Create an HDT file from an RDF file in a tree, stop the chunk creation
	 * with the fluxStop
	 *
	 * @param fluxStop    Flux stopper
	 * @param supplier    HDT supplier to create initial HDT before cat
	 * @param rdfFileName File name.
	 * @param baseURI     Base URI for the dataset.
	 * @param rdfNotation Format of the source RDF File (NTriples, N3,
	 *                    RDF-XML...)
	 * @param hdtFormat   Parameters to tune the generated HDT.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the file cannot be parsed
	 */
	public static HDT catTree(RDFFluxStop fluxStop, HDTSupplier supplier, String rdfFileName, String baseURI,
			RDFNotation rdfNotation, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		return HDTManager.getInstance().doHDTCatTree(fluxStop, supplier, rdfFileName, baseURI, rdfNotation,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT file from an RDF stream, stop the chunk creation with the
	 * fluxStop
	 *
	 * @param fluxStop    Flux stopper
	 * @param supplier    HDT supplier to create initial HDT before cat
	 * @param rdfStream   Stream.
	 * @param baseURI     Base URI for the dataset.
	 * @param rdfNotation Format of the source RDF File (NTriples, N3,
	 *                    RDF-XML...)
	 * @param hdtFormat   Parameters to tune the generated HDT.
	 * @param listener    Listener to get notified of loading progress. Can be
	 *                    null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the file cannot be parsed
	 */
	public static HDT catTree(RDFFluxStop fluxStop, HDTSupplier supplier, InputStream rdfStream, String baseURI,
			RDFNotation rdfNotation, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException {
		return HDTManager.getInstance().doHDTCatTree(fluxStop, supplier, rdfStream, baseURI, rdfNotation,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	/**
	 * Create an HDT from an RDF iterator, stop the chunk creation with the
	 * fluxStop
	 *
	 * @param fluxStop  Flux stopper
	 * @param supplier  HDT supplier to create initial HDT before cat
	 * @param iterator  A provider of triples. Must implement hasNext(), next()
	 *                  and estimatedNumResults.
	 * @param baseURI   Base URI for the dataset.
	 * @param hdtFormat Parameters to tune the generated HDT.
	 * @param listener  Listener to get notified of loading progress. Can be
	 *                  null if no notifications needed.
	 * @return HDT
	 * @throws IOException     when the file cannot be found
	 * @throws ParserException when the file cannot be parsed
	 */
	public static HDT catTree(RDFFluxStop fluxStop, HDTSupplier supplier, Iterator<TripleString> iterator,
			String baseURI, HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException {
		return HDTManager.getInstance().doHDTCatTree(fluxStop, supplier, iterator, baseURI,
				HDTOptions.ofNullable(hdtFormat), ProgressListener.ofNullable(listener));
	}

	// Abstract methods for the current implementation
	protected abstract HDTOptions doReadOptions(String file) throws IOException;

	protected abstract HDT doLoadHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException;

	protected abstract HDT doLoadHDT(InputStream hdtFile, ProgressListener listener, HDTOptions spec)
			throws IOException;

	protected abstract HDT doMapHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException;

	protected abstract HDT doLoadIndexedHDT(String hdtFileName, ProgressListener listener, HDTOptions spec)
			throws IOException;

	protected abstract HDT doLoadIndexedHDT(InputStream hdtFileName, ProgressListener listener, HDTOptions spec)
			throws IOException;

	protected abstract HDT doMapIndexedHDT(String hdtFileName, ProgressListener listener, HDTOptions spec)
			throws IOException;

	protected abstract HDT doIndexedHDT(HDT hdt, ProgressListener listener) throws IOException;

	protected abstract HDT doGenerateHDT(String rdfFileName, String baseURI, RDFNotation rdfNotation,
			HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException;

	protected abstract HDT doGenerateHDT(InputStream fileStream, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException;

	protected abstract HDT doGenerateHDT(Iterator<TripleString> iterator, String baseURI, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException;

	protected abstract HDT doGenerateHDTDisk(String rdfFileName, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException;

	protected abstract HDT doGenerateHDTDisk(InputStream fileStream, String baseURI, RDFNotation rdfNotation,
			CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException;

	protected abstract HDT doGenerateHDTDisk(Iterator<TripleString> iterator, String baseURI, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException, ParserException;

	protected abstract TripleWriter doGetHDTWriter(OutputStream out, String baseURI, HDTOptions hdtFormat)
			throws IOException;

	protected abstract TripleWriter doGetHDTWriter(String outFile, String baseURI, HDTOptions hdtFormat)
			throws IOException;

	protected abstract HDT doHDTCat(String location, String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException;

	protected abstract HDT doHDTCat(List<String> hdtFileNames, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException;

	protected abstract HDT doHDTDiffBitCat(List<String> hdtFileNames, List<? extends Bitmap> deleteBitmaps,
			HDTOptions hdtFormat, ProgressListener listener) throws IOException;

	protected abstract HDT doHDTDiffBitCatObject(List<HDT> hdtFileNames, List<? extends Bitmap> deleteBitmaps,
			HDTOptions hdtFormat, ProgressListener listener, boolean closeHDTs) throws IOException;

	protected abstract HDT doHDTDiff(String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException;

	protected abstract HDT doHDTDiffBit(String location, String hdtFileName, Bitmap deleteBitmap, HDTOptions hdtFormat,
			ProgressListener listener) throws IOException;

	protected abstract HDT doHDTCatTree(RDFFluxStop fluxStop, HDTSupplier supplier, String filename, String baseURI,
			RDFNotation rdfNotation, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException;

	protected abstract HDT doHDTCatTree(RDFFluxStop fluxStop, HDTSupplier supplier, InputStream stream, String baseURI,
			RDFNotation rdfNotation, HDTOptions hdtFormat, ProgressListener listener)
			throws IOException, ParserException;

	protected abstract HDT doHDTCatTree(RDFFluxStop fluxStop, HDTSupplier supplier, Iterator<TripleString> iterator,
			String baseURI, HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException;

}

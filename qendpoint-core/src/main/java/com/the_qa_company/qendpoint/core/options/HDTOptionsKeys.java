package com.the_qa_company.qendpoint.core.options;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVocabulary;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.rdf.RDFFluxStop;
import com.the_qa_company.qendpoint.core.storage.QEPCoreOptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * keys usable with {@link HDTOptions#set(String, String)}
 *
 * @author Antoine Willerval
 */
public class HDTOptionsKeys {
	/**
	 * Key for the compression mode for the {@link HDTManager} generateHDTDisk
	 * methods. Value can be
	 * {@link #LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE} or
	 * {@link #LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE}
	 */
	@Key(type = Key.Type.ENUM, desc = "Compression mode")
	public static final String LOADER_DISK_COMPRESSION_MODE_KEY = "loader.disk.compressMode";
	/**
	 * Value for {@link #LOADER_DISK_COMPRESSION_MODE_KEY}, sort all the file
	 * before going to the next step, slower but decrease the RAM usage. default
	 * config.
	 */
	@Value(key = LOADER_DISK_COMPRESSION_MODE_KEY, desc = "sort all the file before going to the next step, slower but decrease the RAM usage. default config")
	public static final String LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE = "compressionComplete";
	/**
	 * Value for {@link #LOADER_DISK_COMPRESSION_MODE_KEY}, sort while reading
	 * all the file before going to the next step, faster but increase the RAM
	 * usage.
	 */
	@Value(key = LOADER_DISK_COMPRESSION_MODE_KEY, desc = "sort while reading all the file before going to the next step, faster but increase the RAM usage.")
	public static final String LOADER_DISK_COMPRESSION_MODE_VALUE_PARTIAL = "compressionPartial";

	/**
	 * Key for the {@link HDTManager} generateHDTDisk methods, say the number of
	 * workers to merge the data. default to the number of processor. long
	 * value.
	 */
	@Key(type = Key.Type.NUMBER, desc = "Number of core used to compress the HDT")
	public static final String LOADER_DISK_COMPRESSION_WORKER_KEY = "loader.disk.compressWorker";
	/**
	 * Key for the maximum size of a chunk on disk for the {@link HDTManager}
	 * generateHDTDisk methods, the chunk should be in RAM before writing it on
	 * disk and should be sorted. long value.
	 */
	@Key(type = Key.Type.NUMBER, desc = "Maximum size of a chunk")
	public static final String LOADER_DISK_CHUNK_SIZE_KEY = "loader.disk.chunkSize";
	/**
	 * Key for the location of the working directory {@link HDTManager}
	 * generateHDTDisk methods, this directory will be deleted after the HDT
	 * generation. by default, the value is random, it is recommended to set
	 * this option to delete the directory in case of an interruption of the
	 * process. file value.
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the disk generation directory")
	public static final String LOADER_DISK_LOCATION_KEY = "loader.disk.location";
	/**
	 * Key for the location of the future HDT for the {@link HDTManager}
	 * generateHDTDisk methods, this option will create a hdt file after the HDT
	 * generation, the returned HDT will be a mapped HDT of the HDT file.
	 * slower, increase the disk usage, but drastically reduce the RAM usage.
	 * file value.
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the future HDT")
	public static final String LOADER_DISK_FUTURE_HDT_LOCATION_KEY = "loader.disk.futureHDTLocation";
	/**
	 * Key for the maximum number of file opened at the same time, should be
	 * greater than {@link #LOADER_DISK_KWAY_KEY}, 1024 by default
	 */
	@Key(type = Key.Type.NUMBER, desc = "Maximum number of file HDTDisk can open at the same time")
	public static final String LOADER_DISK_MAX_FILE_OPEN_KEY = "loader.disk.maxFileOpen";
	/**
	 * Key for the number of chunk layers opened at the same time, by default
	 * <p>
	 * min(log2(maxFileOpen), chunkSize / (fileBufferSize * compressWorker))
	 * </p>
	 */
	@Key(type = Key.Type.NUMBER, desc = "log of the number of way the system can merge in genDisk")
	public static final String LOADER_DISK_KWAY_KEY = "loader.disk.kway";
	/**
	 * Key for the size of the buffers when opening a file
	 */
	@Key(type = Key.Type.NUMBER, desc = "Size of the file buffers")
	public static final String LOADER_DISK_BUFFER_SIZE_KEY = "loader.disk.fileBufferSize";
	/**
	 * Key for
	 * {@link HDTManager#generateHDTDisk(java.util.Iterator, String, HDTOptions, ProgressListener)},
	 * specify that the method doesn't have to copy the triple strings between 2
	 * calls to the iterator, default false
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "specify that the method doesn't have to copy the triple strings between 2 calls to the iterator")
	public static final String LOADER_DISK_NO_COPY_ITERATOR_KEY = "loader.disk.noCopyIterator";

	/**
	 * Key for the loading mode of a RDF file for the
	 * {@link HDTManager#generateHDT(String, String, RDFNotation, HDTOptions, ProgressListener)}
	 * method, this key isn't working with the other methods. Value can be
	 * {@link #LOADER_TYPE_VALUE_ONE_PASS}, {@link #LOADER_TYPE_VALUE_TWO_PASS},
	 * {@link #LOADER_TYPE_VALUE_CAT} or {@link #LOADER_TYPE_VALUE_DISK}.
	 */
	@Key(type = Key.Type.ENUM, desc = "HDT generation loader type")
	public static final String LOADER_TYPE_KEY = "loader.type";
	/**
	 * Value for {@link #LOADER_TYPE_KEY}, read using disk generation, reduce
	 * the RAM usage and increase disk usage
	 */
	@Value(key = LOADER_TYPE_KEY, desc = "Using genDisk")
	public static final String LOADER_TYPE_VALUE_DISK = "disk";
	/**
	 * Value for {@link #LOADER_TYPE_KEY}, read using HDTCat generation, merge
	 * using HDTCat HDT, reduce the RAM usage
	 */
	@Value(key = LOADER_TYPE_KEY, desc = "Using HDTCat")
	public static final String LOADER_TYPE_VALUE_CAT = "cat";
	/**
	 * Value for {@link #LOADER_TYPE_KEY}, read twice the RDF file, reduce the
	 * RAM usage
	 */
	@Value(key = LOADER_TYPE_KEY, desc = "Using two pass algorithm")
	public static final String LOADER_TYPE_VALUE_TWO_PASS = "two-pass";
	/**
	 * Value for {@link #LOADER_TYPE_KEY}, read only once the RDF file, default
	 * value
	 */
	@Value(key = LOADER_TYPE_KEY, desc = "Using one pass algorithm")
	public static final String LOADER_TYPE_VALUE_ONE_PASS = "one-pass";

	/**
	 * Key for the location of the working directory {@link HDTManager} catTree
	 * methods, this directory will be deleted after the HDT generation. by
	 * default, the value is random, it is recommended to set this option to
	 * delete the directory in case of an interruption of the process. file
	 * value.
	 */
	@Key(type = Key.Type.PATH, desc = "Path of the CatTree generation")
	public static final String LOADER_CATTREE_LOCATION_KEY = "loader.cattree.location";
	/**
	 * Key to use async version of the {@link HDTManager} catTree methods, will
	 * run the k-HDTCAT algorithm, by default the value is false, boolean value
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Use async version")
	public static final String LOADER_CATTREE_ASYNC_KEY = "loader.cattree.async";
	/**
	 * Same as {@link #LOADER_TYPE_KEY} for loader in the CATTREE method
	 */
	@Key(desc = "Loader of the hdt generation")
	public static final String LOADER_CATTREE_LOADERTYPE_KEY = "loader.cattree.loadertype";
	/**
	 * Key for the location of the future HDT for the {@link HDTManager} catTree
	 * methods, this option will create a hdt file after the HDT generation, the
	 * returned HDT will be a mapped HDT of the HDT file. slower, increase the
	 * disk usage, but drastically reduce the RAM usage. file value.
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the future HDT")
	public static final String LOADER_CATTREE_FUTURE_HDT_LOCATION_KEY = "loader.cattree.futureHDTLocation";
	/**
	 * Key for the fault factor for the {@link HDTManager} catTree default value
	 * of the split size of the RDFFluxStop in the generateHDT method.
	 */
	@Key(type = Key.Type.DOUBLE, desc = "Memory fault factor for HDTCat tree method split")
	public static final String LOADER_CATTREE_MEMORY_FAULT_FACTOR = "loader.cattree.memoryFaultFactor";
	/**
	 * Key for the k-merge HDTCat for the {@link HDTManager} catTree default to
	 * 2 using default implementation of HDTCat, not K-HDTCat
	 */
	@Key(type = Key.Type.NUMBER, desc = "Number of HDT to merge at the same time with K-HDTCat, by default it use the default HDTCat implementation")
	public static final String LOADER_CATTREE_KCAT = "loader.cattree.kcat";

	/**
	 * Key for the hdt supplier type, default to memory
	 */
	@Key(type = Key.Type.ENUM, desc = "HDTCat supplier type")
	public static final String HDT_SUPPLIER_KEY = "supplier.type";
	/**
	 * Value for {@link #HDT_SUPPLIER_KEY}, use HDTGenDisk to create the HDT
	 */
	@Value(key = HDT_SUPPLIER_KEY, desc = "using genDisk")
	public static final String LOADER_CATTREE_HDT_SUPPLIER_VALUE_DISK = "disk";
	/**
	 * Value for {@link #HDT_SUPPLIER_KEY}, use the default memory
	 * implementation to create the HDT
	 */
	@Value(key = HDT_SUPPLIER_KEY, desc = "using gen in memory")
	public static final String LOADER_CATTREE_HDT_SUPPLIER_VALUE_MEMORY = "memory";
	/**
	 * Key for the rdf flux stop type, default to the maximum memory allocated
	 */
	@Key(desc = "API use")
	public static final String RDF_FLUX_STOP_KEY = "rdffluxstop.type";
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using
	 * {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final String RDF_FLUX_STOP_VALUE_SIZE = "size";
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using
	 * {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final String RDF_FLUX_STOP_VALUE_COUNT = "count";
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using
	 * {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final String RDF_FLUX_STOP_VALUE_NO_LIMIT = "no_limit";
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using
	 * {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final char RDF_FLUX_STOP_VALUE_OP_AND = '&';
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using
	 * {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final char RDF_FLUX_STOP_VALUE_OP_OR = '|';
	/**
	 * Value type for the {@link #RDF_FLUX_STOP_KEY}, using
	 * {@link RDFFluxStop#asConfig()} would be easier
	 */
	public static final char RDF_FLUX_STOP_VALUE_OP_NOT = '!';

	/**
	 * Key for enabling the profiler (if implemented), default to false. Boolean
	 * value
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Use the profiler to get the time of each section")
	public static final String PROFILER_KEY = "profiler";
	/**
	 * Key for the profiler output (if implemented). File value
	 */
	@Key(type = Key.Type.PATH, desc = "Profiler output file")
	public static final String PROFILER_OUTPUT_KEY = "profiler.output";
	/**
	 * Key for enabling the profiler (if implemented) for async bi tasks,
	 * default to false. Boolean value
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Run a second profiler for async bi tasks")
	public static final String PROFILER_ASYNC_KEY = "profiler.async";
	/**
	 * Key for the profiler output (if implemented). File value
	 */
	@Key(type = Key.Type.PATH, desc = "Profiler output file")
	public static final String PROFILER_ASYNC_OUTPUT_KEY = "profiler.async.output";
	/**
	 * Key for enabling the canonical NTriple file simple parser, default to
	 * false. Boolean value
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Use the canonical NT file parser, removing checks")
	public static final String NT_SIMPLE_PARSER_KEY = "parser.ntSimpleParser";
	/**
	 * Key for setting the maximum amount of file loaded with the directory
	 * parser, 1 for no async parsing, 0 for the number of processors, default
	 * 1. Number value
	 */
	@Key(type = Key.Type.NUMBER, desc = "Use async dir parser")
	public static final String ASYNC_DIR_PARSER_KEY = "parser.dir.async";
	/**
	 * Key for setting the triple order. see {@link TripleComponentOrder}'s
	 * names to have the values default to {@link TripleComponentOrder#SPO}
	 */
	@Key(type = Key.Type.STRING, desc = "HDT generation triple order")
	public static final String TRIPLE_ORDER_KEY = "triplesOrder";

	/**
	 * Option to set how the HDTs are loaded in HDTCat/HDTDiff, default
	 * {@link #LOAD_HDT_TYPE_VALUE_MAP}
	 */
	@Key(type = Key.Type.ENUM, desc = "loading type for HDTCat / HDTDiff")
	public static final String LOAD_HDT_TYPE_KEY = "loader.hdt.type";
	/**
	 * load the HDT file into memory
	 */
	@Value(key = LOAD_HDT_TYPE_KEY, desc = "load the HDTs in memory")
	public static final String LOAD_HDT_TYPE_VALUE_LOAD = "load";
	/**
	 * map the HDT file, default value
	 */
	@Value(key = LOAD_HDT_TYPE_KEY, desc = "map the HDTs")
	public static final String LOAD_HDT_TYPE_VALUE_MAP = "map";

	/**
	 * Implementation of the temporary dictionary
	 */
	@Key(type = Key.Type.ENUM, desc = "Internal temporary dictionary")
	public static final String TEMP_DICTIONARY_IMPL_KEY = "tempDictionary.impl";
	/**
	 * use Hash map to create the HDT
	 */
	@Value(key = TEMP_DICTIONARY_IMPL_KEY, desc = "hash dictionary")
	public static final String TEMP_DICTIONARY_IMPL_VALUE_HASH = "hash";
	/**
	 * use Hash quad to create the HDTQ
	 */
	public static final String TEMP_DICTIONARY_IMPL_VALUE_HASH_QUAD = "hashQuad";
	/**
	 * use Hash map to create the HDT and store the multisection dictionary,
	 * mandatory to create MSC
	 */
	@Value(key = TEMP_DICTIONARY_IMPL_KEY, desc = "hash dictionary with literal count")
	public static final String TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH = "multHash";
	/**
	 * use Hash map with Prefix AND Suffix front-coded (PSFC), mandatory to
	 * create PSFC dictionary
	 */
	@Value(key = TEMP_DICTIONARY_IMPL_KEY, desc = "Prefix AND Suffix front-coded (PSFC) hash dictionary")
	public static final String TEMP_DICTIONARY_IMPL_VALUE_HASH_PSFC = "hashPsfc";

	/**
	 * Implementation of the dictionary
	 */
	@Key(type = Key.Type.ENUM, desc = "HDT dictionary type")
	public static final String DICTIONARY_TYPE_KEY = "dictionary.type";
	/**
	 * 4 Section dictionary
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Four section dictionary")
	public static final String DICTIONARY_TYPE_VALUE_FOUR_SECTION = HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION;
	/*
	 * 4 Quad Section dictionary
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Four quad section dictionary")
	public static final String DICTIONARY_TYPE_VALUE_FOUR_QUAD_SECTION = HDTVocabulary.DICTIONARY_TYPE_FOUR_QUAD_SECTION;
	/**
	 * Prefix AND Suffix front-coded (PSFC) 4 Section dictionary
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Prefix AND Suffix front-coded (PSFC) four section dictionary")
	public static final String DICTIONARY_TYPE_VALUE_FOUR_PSFC_SECTION = HDTVocabulary.DICTIONARY_TYPE_FOUR_PSFC_SECTION;
	/**
	 * big 4 Section dictionary
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Four section dictionary big")
	public static final String DICTIONARY_TYPE_VALUE_FOUR_SECTION_BIG = "dictionaryFourBig";
	/**
	 * multi section dictionary
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Multi section dictionary")
	public static final String DICTIONARY_TYPE_VALUE_MULTI_OBJECTS = "dictionaryMultiObj";
	/**
	 * multi section dictionary lang
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Multi section dictionary language")
	public static final String DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG = "dictionaryMultiObjLang";
	/**
	 * multi section dictionary lang quad
	 */
	@Value(key = DICTIONARY_TYPE_KEY, desc = "Multi section dictionary language")
	public static final String DICTIONARY_TYPE_VALUE_MULTI_OBJECTS_LANG_QUAD = "dictionaryMultiObjLangQuad";

	/**
	 * Ignore MSDL RDFTYPE INDEX
	 */
	@Key(type = Key.Type.ENUM, desc = "Ignore MSDL RDFTYPE INDEX")
	public static final String DICTIONARY_MSDL_NO_RDFTYPE_INDEX = "dictionary.msdl.nordftypeindex";

	/**
	 * Location of the HDTCat temp files
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the HDTCat temp files")
	public static final String HDTCAT_LOCATION = "hdtcat.location";

	/**
	 * Use legacy HDTCat/HDTDiff
	 */
	@Key(type = Key.Type.PATH, desc = "Use legacy HDTCat")
	public static final String HDTCAT_LEGACY = "hdtcat.legacy";
	/**
	 * Location of the HDTCat hdt after the loading
	 */
	@Key(type = Key.Type.PATH, desc = "Location of the HDTCat hdt after the loading")
	public static final String HDTCAT_FUTURE_LOCATION = "hdtcat.location.future";
	/**
	 * Delete the HDTCat temp files directory after HDTCat
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Delete the HDTCat temp files directory after HDTCat, default to true")
	public static final String HDTCAT_DELETE_LOCATION = "hdtcat.deleteLocation";

	/**
	 * Use disk implementation to generate the hdt sub-index, default false
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Use disk implementation to generate the hdt sub-index, default false")
	public static final String BITMAPTRIPLES_SEQUENCE_DISK = "bitmaptriples.sequence.disk";

	/**
	 * Use disk 375 bitmap subindex implementation to generate the HDT sub
	 * index, default false
	 */
	@Key(type = Key.Type.BOOLEAN, desc = "Use disk 375 subindex implementation to generate the hdt sub-index, default false")
	public static final String BITMAPTRIPLES_SEQUENCE_DISK_SUBINDEX = "bitmaptriples.sequence.disk.subindex";

	/**
	 * disk location for the {@link #BITMAPTRIPLES_SEQUENCE_DISK} option
	 */
	@Key(type = Key.Type.PATH, desc = "Disk location for the " + BITMAPTRIPLES_SEQUENCE_DISK + " option")
	public static final String BITMAPTRIPLES_SEQUENCE_DISK_LOCATION = "bitmaptriples.sequence.disk.location";

	/**
	 * Bitmap type for the Y bitmap, default
	 * {@link HDTVocabulary#BITMAP_TYPE_PLAIN}
	 */
	@Key(type = Key.Type.STRING, desc = "Bitmap type for the Y bitmap, default " + HDTVocabulary.BITMAP_TYPE_PLAIN)
	public static final String BITMAPTRIPLES_BITMAP_Y = "bitmap.y";

	/**
	 * Bitmap type for the Z bitmap, default
	 * {@link HDTVocabulary#BITMAP_TYPE_PLAIN}
	 */
	@Key(type = Key.Type.STRING, desc = "Bitmap type for the Z bitmap, default " + HDTVocabulary.BITMAP_TYPE_PLAIN)
	public static final String BITMAPTRIPLES_BITMAP_Z = "bitmap.z";

	/**
	 * Sequence type for the Y sequence, default
	 * {@link HDTVocabulary#SEQ_TYPE_LOG}
	 */
	@Key(type = Key.Type.STRING, desc = "Sequence type for the Y sequence, default " + HDTVocabulary.SEQ_TYPE_LOG)
	public static final String BITMAPTRIPLES_SEQ_Y = "seq.y";

	/**
	 * Sequence type for the Z sequence, default
	 * {@link HDTVocabulary#SEQ_TYPE_LOG}
	 */
	@Key(type = Key.Type.STRING, desc = "Sequence type for the Z sequence, default " + HDTVocabulary.SEQ_TYPE_LOG)
	public static final String BITMAPTRIPLES_SEQ_Z = "seq.z";

	/**
	 * Indexing method for the bitmap triples, default
	 * {@link #BITMAPTRIPLES_INDEX_METHOD_VALUE_RECOMMENDED}
	 */
	@Key(type = Key.Type.ENUM, desc = "Indexing method for the bitmap triples")
	public static final String BITMAPTRIPLES_INDEX_METHOD_KEY = "bitmaptriples.indexmethod";

	/**
	 * value for {@link #BITMAPTRIPLES_INDEX_METHOD_KEY}. Recommended
	 * implementation, default value
	 */
	@Value(key = BITMAPTRIPLES_INDEX_METHOD_KEY, desc = "Recommended implementation, default value")
	public static final String BITMAPTRIPLES_INDEX_METHOD_VALUE_RECOMMENDED = "recommended";

	/**
	 * value for {@link #BITMAPTRIPLES_INDEX_METHOD_KEY}. Legacy implementation,
	 * fast, but memory inefficient
	 */
	@Value(key = BITMAPTRIPLES_INDEX_METHOD_KEY, desc = "Legacy implementation, fast, but memory inefficient")
	public static final String BITMAPTRIPLES_INDEX_METHOD_VALUE_LEGACY = "legacy";

	/**
	 * value for {@link #BITMAPTRIPLES_INDEX_METHOD_KEY}. Disk option, handle
	 * the indexing on disk to reduce usage
	 */
	@Value(key = BITMAPTRIPLES_INDEX_METHOD_KEY, desc = "Disk option, handle the indexing on disk to reduce usage")
	public static final String BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK = "disk";
	/**
	 * value for {@link #BITMAPTRIPLES_INDEX_METHOD_KEY}. Memory optimized
	 * option
	 */
	@Value(key = BITMAPTRIPLES_INDEX_METHOD_KEY, desc = "Memory optimized option")
	public static final String BITMAPTRIPLES_INDEX_METHOD_VALUE_OPTIMIZED = "optimized";

	/**
	 * Key for the {@link HDTManager} loadIndexed methods, say the number of
	 * workers to merge the data. default to the number of processor. long
	 * value.
	 */
	@Key(type = Key.Type.NUMBER, desc = "Number of core used to index the HDT with "
			+ BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK + " index method.")
	public static final String BITMAPTRIPLES_DISK_WORKER_KEY = "bitmaptriples.indexmethod.disk.compressWorker";
	/**
	 * Key for the maximum size of a chunk on disk for the {@link HDTManager}
	 * generateHDTDisk methods, the chunk should be in RAM before writing it on
	 * disk and should be sorted. long value.
	 */
	@Key(type = Key.Type.NUMBER, desc = "Maximum size of a chunk")
	public static final String BITMAPTRIPLES_DISK_CHUNK_SIZE_KEY = "bitmaptriples.indexmethod.disk.chunkSize";
	/**
	 * Key for the size of the buffers when opening a file
	 */
	@Key(type = Key.Type.NUMBER, desc = "Size of the file buffers")
	public static final String BITMAPTRIPLES_DISK_BUFFER_SIZE_KEY = "bitmaptriples.indexmethod.disk.fileBufferSize";
	/**
	 * Key for the maximum number of file opened at the same time, should be
	 * greater than {@link #BITMAPTRIPLES_DISK_KWAY_KEY}, 1024 by default
	 */
	@Key(type = Key.Type.NUMBER, desc = "Maximum number of file " + BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK
			+ " index method can open at the same time")
	public static final String BITMAPTRIPLES_DISK_MAX_FILE_OPEN_KEY = "bitmaptriples.indexmethod.disk.maxFileOpen";
	/**
	 * Key for the number of chunk layers opened at the same time, by default
	 * <p>
	 * min(log2(maxFileOpen), chunkSize / (fileBufferSize * compressWorker))
	 * </p>
	 */
	@Key(type = Key.Type.NUMBER, desc = "log of the number of way the system can merge in "
			+ BITMAPTRIPLES_INDEX_METHOD_VALUE_DISK + " index method")
	public static final String BITMAPTRIPLES_DISK_KWAY_KEY = "bitmaptriples.indexmethod.disk.kway";

	@Key(type = Key.Type.BOOLEAN, desc = "Dump binary offsets, default false")
	public static final String DUMP_BINARY_OFFSETS = "bump.binary.offsets";

	@Key(type = Key.Type.STRING, desc = "Create other indexes in bitmaptriples pattern values (spo, ops, etc.), default none")
	public static final String BITMAPTRIPLES_INDEX_OTHERS = "bitmaptriples.index.others";

	@Key(type = Key.Type.BOOLEAN, desc = "No FoQ index generation default false")
	public static final String BITMAPTRIPLES_INDEX_NO_FOQ = "bitmaptriples.index.noFoQ";

	// use tree-map to have a better order
	private static final Map<String, Option> OPTION_MAP = new TreeMap<>();

	static {
		try {
			registerOptionsClass(HDTOptionsKeys.class);
			registerOptionsClass(QEPCoreOptions.class);
		} catch (Exception e) {
			throw new Error("Can't load option keys", e);
		}
	}

	/**
	 * register an options class for the {@link #getOptionMap()} method, will
	 * read all the public static fields with {@link Key} and {@link Value}
	 *
	 * @param cls class
	 * @throws Exception register exception
	 */
	public static void registerOptionsClass(Class<?> cls) throws Exception {
		for (Field f : cls.getDeclaredFields()) {
			if ((f.getModifiers() & Modifier.STATIC) == 0 || (f.getModifiers() & Modifier.PUBLIC) == 0) {
				continue; // no static modifier
			}
			Key key = f.getAnnotation(Key.class);
			if (key != null) {
				String keyValue = String.valueOf(f.get(null));

				OPTION_MAP.put(keyValue, new Option(keyValue, key));
			} else {
				Value value = f.getAnnotation(Value.class);
				if (value != null) {
					String valueValue = String.valueOf(f.get(null));
					Option opt = OPTION_MAP.get(value.key());
					if (opt != null) {
						opt.values.add(new OptionValue(valueValue, value));
					}
				}
			}
		}
	}

	public static Map<String, Option> getOptionMap() {
		return Collections.unmodifiableMap(OPTION_MAP);
	}

	private HDTOptionsKeys() {
	}

	public static class OptionValue {
		private final String value;
		private final Value valueInfo;

		private OptionValue(String value, Value valueInfo) {
			this.value = value;
			this.valueInfo = valueInfo;
		}

		public String getValue() {
			return value;
		}

		public Value getValueInfo() {
			return valueInfo;
		}
	}

	public static class Option {
		private final String key;
		private final Key keyInfo;
		private final List<OptionValue> values = new ArrayList<>();

		private Option(String key, Key keyInfo) {
			this.key = key;
			this.keyInfo = keyInfo;
		}

		public String getKey() {
			return key;
		}

		public Key getKeyInfo() {
			return keyInfo;
		}

		public List<OptionValue> getValues() {
			return Collections.unmodifiableList(values);
		}
	}
}

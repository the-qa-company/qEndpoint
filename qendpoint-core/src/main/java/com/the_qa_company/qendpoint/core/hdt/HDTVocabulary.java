/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/hdt/
 * HDTVocabulary.java $ Revision: $Rev: 191 $ Last modified: $Date: 2013-03-03
 * 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author: mario.arias $
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version. This library is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy of
 * the GNU Lesser General Public License along with this library; if not, write
 * to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston,
 * MA 02110-1301 USA Contacting the authors: Mario Arias: mario.arias@deri.org
 * Javier D. Fernandez: jfergar@infor.uva.es Miguel A. Martinez-Prieto:
 * migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.hdt;

/**
 * @author mario.arias
 */
public class HDTVocabulary {
	// Base
	public static final String HDT_BASE = "<http://purl.org/HDT/hdt#";
	public static final String HDT_CONTAINER = HDT_BASE + "HDTv" + HDTVersion.HDT_VERSION + ">";
	public static final String HDT_CONTAINER_2 = HDT_BASE + "HDTv" + HDTVersion.HDT_VERSION_2 + ">";

	public static final String HDT_HEADER = HDT_BASE + "header";
	public static final String HDT_DICTIONARY_BASE = HDT_BASE + "dictionary";
	public static final String HDT_DICTIONARY = HDT_DICTIONARY_BASE + ">";
	public static final String HDT_TRIPLES_BASE = HDT_BASE + "triples";
	public static final String HDT_TRIPLES = HDT_TRIPLES_BASE + ">";
	public static final String HDT_SEQ_BASE = HDT_BASE + "seq";
	public static final String HDT_BITMAP_BASE = HDT_BASE + "bitmap";

	// External Vocabularies
	public static final String RDF = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	public static final String RDF_TYPE = RDF + "type>";
	public static final String DUBLIN_CORE = "<http://purl.org/dc/terms/";
	public static final String DUBLIN_CORE_ISSUED = DUBLIN_CORE + "issued>";

	// VOID
	public static final String VOID_BASE = "<http://rdfs.org/ns/void#";
	public static final String VOID_DATASET = VOID_BASE + "Dataset>";
	public static final String VOID_TRIPLES = VOID_BASE + "triples>";
	public static final String VOID_ENTITIES = VOID_BASE + "entities>";
	public static final String VOID_CLASSES = VOID_BASE + "classes>";
	public static final String VOID_PROPERTIES = VOID_BASE + "properties>";
	public static final String VOID_DISTINCT_SUBJECTS = VOID_BASE + "distinctSubjects>";
	public static final String VOID_DISTINCT_OBJECTS = VOID_BASE + "distinctObjects>";

	// Header
	public static final String HEADER_NTRIPLES = "ntriples";
	public static final String HDT_DATASET = HDT_BASE + "Dataset>";
	public static final String HDT_FORMAT_INFORMATION = HDT_BASE + "formatInformation>";
	public static final String HDT_STATISTICAL_INFORMATION = HDT_BASE + "statisticalInformation>";
	public static final String HDT_PUBLICATION_INFORMATION = HDT_BASE + "publicationInformation>";

	// Dictionary
	public static final String DICTIONARY_TYPE = DUBLIN_CORE + "format>";
	public static final String DICTIONARY_NUMSUBJECTS = HDT_DICTIONARY_BASE + "numSubjects>";
	public static final String DICTIONARY_NUMPREDICATES = HDT_DICTIONARY_BASE + "numPredicates>";
	public static final String DICTIONARY_NUMOBJECTS = HDT_DICTIONARY_BASE + "numObjects>";
	public static final String DICTIONARY_NUMSHARED = HDT_DICTIONARY_BASE + "numSharedSubjectObject>";
	public static final String DICTIONARY_MAXSUBJECTID = HDT_DICTIONARY_BASE + "maxSubjectID>";
	public static final String DICTIONARY_MAXPREDICATEID = HDT_DICTIONARY_BASE + "maxPredicateID>";
	public static final String DICTIONARY_MAXOBJECTTID = HDT_DICTIONARY_BASE + "maxObjectID>";
	public static final String DICTIONARY_SIZE_STRINGS = HDT_DICTIONARY_BASE + "sizeStrings>";
	public static final String DICTIONARY_BLOCK_SIZE = HDT_DICTIONARY_BASE + "blockSize>";

	// Dictionary Types
	public static final String DICTIONARY_TYPE_PLAIN = HDT_DICTIONARY_BASE + "Plain>";
	public static final String DICTIONARY_TYPE_FOUR_SECTION = HDT_DICTIONARY_BASE + "Four>";
	public static final String DICTIONARY_TYPE_MULT_SECTION = HDT_DICTIONARY_BASE + "Mult>";
	public static final String DICTIONARY_TYPE_FOUR_QUAD_SECTION = HDT_DICTIONARY_BASE + "FourQuad>";
	public static final String DICTIONARY_TYPE_MULT_SECTION_LANG = HDT_DICTIONARY_BASE + "MultLang>";
	public static final String DICTIONARY_TYPE_MULT_SECTION_LANG_QUAD = HDT_DICTIONARY_BASE + "MultLangQuad>";

	public static final String DICTIONARY_TYPE_FOUR_PSFC_SECTION = HDT_DICTIONARY_BASE + "FourPsfc>";

	// Triples
	public static final String TRIPLES_TYPE = DUBLIN_CORE + "format>";
	public static final String TRIPLES_NUM_TRIPLES = HDT_TRIPLES_BASE + "numTriples>";
	public static final String TRIPLES_ORDER = HDT_TRIPLES_BASE + "Order>";
	public static final String TRIPLES_SEQX_TYPE = HDT_TRIPLES_BASE + "seqX>";
	public static final String TRIPLES_SEQY_TYPE = HDT_TRIPLES_BASE + "seqY>";
	public static final String TRIPLES_SEQZ_TYPE = HDT_TRIPLES_BASE + "seqZ>";
	public static final String TRIPLES_SEQX_SIZE = HDT_TRIPLES_BASE + "seqXsize>";
	public static final String TRIPLES_SEQY_SIZE = HDT_TRIPLES_BASE + "seqYsize>";
	public static final String TRIPLES_SEQZ_SIZE = HDT_TRIPLES_BASE + "seqZsize>";
	public static final String TRIPLES_BITMAPX_SIZE = HDT_TRIPLES_BASE + "bitmapXsize>";
	public static final String TRIPLES_BITMAPY_SIZE = HDT_TRIPLES_BASE + "bitmapYsize>";
	public static final String TRIPLES_BITMAPZ_SIZE = HDT_TRIPLES_BASE + "bitmapZsize>";

	// Triples types
	public static final String TRIPLES_TYPE_TRIPLESLIST = HDT_TRIPLES_BASE + "List>";
	public static final String TRIPLES_TYPE_PLAIN = HDT_TRIPLES_BASE + "Plain>";
	public static final String TRIPLES_TYPE_COMPACT = HDT_TRIPLES_BASE + "Compact>";
	public static final String TRIPLES_TYPE_BITMAP = HDT_TRIPLES_BASE + "Bitmap>";
	public static final String TRIPLES_TYPE_BITMAP_QUAD = HDT_TRIPLES_BASE + "BitmapQuad>";

	// Index type
	public static final String INDEX_TYPE_FOQ = HDT_BASE + "indexFoQ>";

	// Sequences
	public static final String SEQ_TYPE_INT32 = HDT_SEQ_BASE + "Int32>";
	public static final String SEQ_TYPE_INT64 = HDT_SEQ_BASE + "Int64>";
	public static final String SEQ_TYPE_LOG = HDT_SEQ_BASE + "Log>";
	public static final String SEQ_TYPE_HUFFMAN = HDT_SEQ_BASE + "Huffman>";
	public static final String SEQ_TYPE_WAVELET = HDT_SEQ_BASE + "Wavelet>";

	// Bitmaps
	public static final String BITMAP_TYPE_PLAIN = HDT_BITMAP_BASE + "Plain>";
	public static final String BITMAP_TYPE_ROARING = HDT_BITMAP_BASE + "Roaring>";
	public static final String BITMAP_TYPE_ROARING_MULTI = HDT_BITMAP_BASE + "RoaringMulti>";

	// Misc
	public static final String ORIGINAL_SIZE = HDT_BASE + "originalSize>";
	public static final String HDT_SIZE = HDT_BASE + "hdtSize>";

	private HDTVocabulary() {
	}
}

package com.the_qa_company.qendpoint.core.hdtCat;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdtCat.utils.Utility;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.options.HDTSpecification;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import org.apache.commons.io.file.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class HdtCatLiteralsTest extends AbstractMapMemoryTest implements ProgressListener {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object> params() {
		return List.of(HDTOptionsKeys.LOAD_HDT_TYPE_VALUE_MAP, HDTOptionsKeys.LOAD_HDT_TYPE_VALUE_LOAD);
	}

	@Parameterized.Parameter
	public String loadingMethod;

	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	private void help(String filename1, String filename2, String concatFilename) throws ParserException, IOException {
		ClassLoader classLoader = getClass().getClassLoader();
		URL resource = classLoader.getResource(filename1);
		if (resource == null) {
			throw new FileNotFoundException(filename1);
		}
		String file1 = resource.getFile();
		URL resource1 = classLoader.getResource(filename2);
		if (resource1 == null) {
			throw new FileNotFoundException(filename2);
		}
		String file2 = resource1.getFile();
		URL resource2 = classLoader.getResource(concatFilename);
		if (resource2 == null) {
			throw new FileNotFoundException(concatFilename);
		}
		String concat = resource2.getFile();

		String hdt1Location = tempDir.newFile().getAbsolutePath();
		String hdt2Location = tempDir.newFile().getAbsolutePath();

		HDTSpecification spec = new HDTSpecification();
		spec.set(HDTOptionsKeys.TEMP_DICTIONARY_IMPL_KEY, HDTOptionsKeys.TEMP_DICTIONARY_IMPL_VALUE_MULT_HASH);
		spec.set(HDTOptionsKeys.DICTIONARY_TYPE_KEY, HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS);
		spec.set(HDTOptionsKeys.LOAD_HDT_TYPE_KEY, loadingMethod);

		try (HDT hdt = HDTManager.generateHDT(new File(file1).getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec,
				this)) {
			hdt.saveToHDT(hdt1Location, null);
		}
		try (HDT hdt = HDTManager.generateHDT(new File(file2).getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec,
				this)) {
			hdt.saveToHDT(hdt2Location, null);
		}
		File file = tempDir.newFile();
		File theDir = tempDir.newFolder();
		Files.createDirectories(theDir.toPath());

		try (HDT hdtCatNew = HDTManager.catHDT(theDir.getAbsolutePath(), hdt1Location, hdt2Location, spec, null)) {
			hdtCatNew.saveToHDT(file.getAbsolutePath() + "_cat.hdt", null);
		}

		try (HDT hdtCatOld = HDTManager.generateHDT(new File(concat).getAbsolutePath(), "uri", RDFNotation.NTRIPLES,
				spec, this); HDT hdtCatNew = HDTManager.mapIndexedHDT(file.getAbsolutePath() + "_cat.hdt")) {
			Utility.compareCustomDictionary(hdtCatOld.getDictionary(), hdtCatNew.getDictionary());
			Utility.compareTriples(hdtCatOld, hdtCatNew);
		} finally {
			PathUtils.deleteDirectory(theDir.toPath());
		}
	}

	// @Test
//    public void misc(){
//        String file1 = "/Users/alyhdr/Desktop/qa-company/data/admin/eu/hdt_index/new_index_diff.hdt";
//        String file2 = "/Users/alyhdr/Desktop/qa-company/data/admin/eu/hdt_index/new_index_v2.hdt";
//
//        try {
//            HDTSpecification spec = new HDTSpecification();
//            spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
//            HDT hdt1 = HDTManager.mapHDT(file1);
//            HDT hdt2 = HDTManager.mapHDT(file2);
//            System.out.println(hdt1.getDictionary().stringToId("https://linkedopendata.eu/entity/Q3048056",TripleComponentRole.OBJECT));
//            IteratorTripleID search1 = hdt1.getTriples().search(new TripleID(0, 0, 12645790));
//            if(search1.hasNext()){
//                System.out.println(search1.next());
//            }
//            IteratorTripleString search = hdt1.search("", "", "https://linkedopendata.eu/entity/Q3048056");
//            if(search.hasNext()){
//                System.out.println(search.next());
//            }
//
//            Iterator<? extends CharSequence> no_datatype1 = hdt1.getDictionary().getAllObjects().get(LiteralsUtils.NO_DATATYPE).getSortedEntries();
//            Iterator<? extends CharSequence> no_datatype2 = hdt2.getDictionary().getAllObjects().get(LiteralsUtils.NO_DATATYPE).getSortedEntries();
//            while (no_datatype1.hasNext()){
//                CharSequence next1 = no_datatype1.next();
//                CharSequence next2 = no_datatype2.next();
//                System.out.println(next1+" "+next2);
//            }
////            for (Map.Entry<String, DictionarySection> section : hdt.getDictionary().getAllObjects().entrySet()) {
////                System.out.println("Checking section: "+section.getKey());
////                Iterator<? extends CharSequence> sortedEntries = section.getValue().getSortedEntries();
////                HashSet<CharSequence> set = new HashSet<>();
////                while (sortedEntries.hasNext()) {
////                    CharSequence next = sortedEntries.next();
////                    if (set.contains(next)) {
////                        System.out.println("Found duplicate: " + next);
////                    } else {
////                        set.add(next);
////                    }
////                }
////            }
//        } catch (IOException | NotFoundException e) {
//            e.printStackTrace();
//        }
//
//    }
	@Test
	public void cat1() throws ParserException, IOException {
		help("example1.nt", "example2.nt", "example1+2.nt");
	}

	@Test
	public void cat2() throws ParserException, IOException {
		help("example2.nt", "example3.nt", "example2+3.nt");
	}

	@Test
	public void cat3() throws ParserException, IOException {
		help("example4.nt", "example5.nt", "example4+5.nt");
	}

	@Test
	public void cat4() throws ParserException, IOException {
		help("example6.nt", "example7.nt", "example6+7.nt");
	}

	@Test
	public void cat5() throws ParserException, IOException {
		help("example8.nt", "example9.nt", "example8+9.nt");
	}

	@Test
	public void cat6() throws ParserException, IOException {
		help("example10.nt", "example11.nt", "example10+11.nt");
	}

	@Test
	public void cat7() throws ParserException, IOException {
		help("example12.nt", "example13.nt", "example12+13.nt");
	}

	@Test
	public void cat9() throws ParserException, IOException {
		help("example14.nt", "example15.nt", "example14+15.nt");
	}

	@Test
	public void cat10() throws ParserException, IOException {
		help("example16.nt", "example17.nt", "example16+17.nt");
	}

	@Test
	public void cat11() throws ParserException, IOException {
		help("example1.nt", "example1.nt", "example1.nt");
	}

	@Test
	public void cat12() throws ParserException, IOException {
		help("example18.nt", "example19.nt", "example18+19.nt");
	}

	@Test
	public void cat13() throws ParserException, IOException {
		help("example20.nt", "example21.nt", "example20+21.nt");
	}

	@Test
	public void cat14() throws ParserException, IOException {
		help("example22.nt", "example23.nt", "example22+23.nt");
	}

	@Test
	public void cat15() throws ParserException, IOException {
		help("example24.nt", "example25.nt", "example24+25.nt");
	}

	@Override
	public void notifyProgress(float level, String message) {

	}

}

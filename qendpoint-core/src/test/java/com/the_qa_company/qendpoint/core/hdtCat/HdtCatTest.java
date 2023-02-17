package com.the_qa_company.qendpoint.core.hdtCat;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdtCat.utils.Utility;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.util.io.AbstractMapMemoryTest;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

public class HdtCatTest extends AbstractMapMemoryTest implements ProgressListener {

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

		String hdt1Location = file1.replace(".nt", ".hdt");
		String hdt2Location = file2.replace(".nt", ".hdt");
		HDTOptions spec = HDTOptions.of(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY, true);
		try (HDT hdt = HDTManager.generateHDT(new File(file1).getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec,
				this)) {
			hdt.saveToHDT(hdt1Location, null);
		}
		try (HDT hdt = HDTManager.generateHDT(new File(file2).getAbsolutePath(), "uri", RDFNotation.NTRIPLES, spec,
				this)) {
			hdt.saveToHDT(hdt2Location, null);
		}
		try (HDT hdtCatOld = HDTManager.generateHDT(new File(concat).getAbsolutePath(), "uri", RDFNotation.NTRIPLES,
				spec, this)) {

			File file = new File(file1);
			File theDir = new File(file.getAbsolutePath() + "_tmp");
			Files.createDirectories(theDir.toPath());
			try (HDT hdtCatNew = HDTManager.catHDT(theDir.getAbsolutePath(), hdt1Location, hdt2Location, spec, null)) {

				// HDTCat hdtCatNew = new HDTCat(new
				// HDTSpecification(),hdt1,hdt2,this);
				Utility.compareDictionary(hdtCatOld.getDictionary(), hdtCatNew.getDictionary());
				Utility.compareTriples(hdtCatOld, hdtCatNew);
			}
			Files.delete(theDir.toPath());
		}
	}

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

	@Override
	public void notifyProgress(float level, String message) {

	}

}

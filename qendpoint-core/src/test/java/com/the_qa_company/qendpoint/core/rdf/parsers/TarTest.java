package com.the_qa_company.qendpoint.core.rdf.parsers;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import com.the_qa_company.qendpoint.core.storage.TempBuffIn;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import com.the_qa_company.qendpoint.core.util.io.CountInputStream;
import com.the_qa_company.qendpoint.core.util.io.NonCloseInputStream;

public class TarTest {

	public static void main(String[] args) throws Throwable {

		InputStream input = new CountInputStream(
				new TempBuffIn(new GZIPInputStream(new FileInputStream("/Users/mck/rdf/dataset/tgztest.tar.gz"))));
//		InputStream input = new CountInputStream(new TempBuffIn(new FileInputStream("/Users/mck/rdf/dataset/tgztest.tar")));
		final TarArchiveInputStream debInputStream = new TarArchiveInputStream(input);
		TarArchiveEntry entry;

		while ((entry = (TarArchiveEntry) debInputStream.getNextEntry()) != null) {
			System.out.println(entry.getName());

		}

		debInputStream.close();
	}

}

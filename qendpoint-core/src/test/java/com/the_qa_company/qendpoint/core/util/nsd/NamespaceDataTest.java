package com.the_qa_company.qendpoint.core.util.nsd;

import org.apache.commons.io.file.PathUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class NamespaceDataTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void loadSaveLoad() throws IOException {
		Path p = tempDir.newFolder().toPath();

		try {
			Path f = p.resolve("ns.nsd");
			NamespaceData nsd = new NamespaceData(f);

			nsd.load();

			nsd.setNamespace("ex", "http://example.org/#");
			nsd.setNamespace("ex2", "http://example2.org/#");

			nsd.sync();

			NamespaceData nsd2 = new NamespaceData(f);

			nsd2.load();

			assertEquals(2, nsd.namespaces.size());
			assertEquals(nsd.namespaces, nsd2.namespaces);

			nsd2.removeNamespace("ex");

			nsd2.sync();

			NamespaceData nsd3 = new NamespaceData(f);

			nsd3.load();

			assertEquals(1, nsd2.namespaces.size());
			assertEquals(nsd2.namespaces, nsd3.namespaces);
		} finally {
			PathUtils.deleteDirectory(p);
		}
	}
}
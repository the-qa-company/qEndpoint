package com.the_qa_company.qendpoint.core.hdt.impl.diskimport;

import com.the_qa_company.qendpoint.core.util.io.CloseSuppressPath;
import com.the_qa_company.qendpoint.core.util.io.compress.CompressUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class CompressTripleMapperBulkMappingTest {
	@Rule
	public TemporaryFolder tempDir = TemporaryFolder.builder().assureDeletion().build();

	@Test
	public void bulkMappingsSetValues() throws Exception {
		try (CloseSuppressPath root = CloseSuppressPath.of(tempDir.newFolder().toPath())) {
			root.closeWithDeleteRecurse();

			try (CloseSuppressPath mapperDir = root.resolve("mapper")) {
				mapperDir.mkdirs();

				long tripleCount = 5;
				CompressTripleMapper mapper = new CompressTripleMapper(mapperDir, tripleCount, 1024, true, tripleCount);

				long[] preMapIds = new long[(int) tripleCount + 1];
				long[] newMapIds = new long[preMapIds.length];

				for (int i = 1; i < preMapIds.length; i++) {
					preMapIds[i] = i;
					newMapIds[i] = CompressUtil.getHeaderId(i);
				}

				mapper.onSubject(preMapIds, newMapIds, 1, (int) tripleCount);
				mapper.onPredicate(preMapIds, newMapIds, 1, (int) tripleCount);
				mapper.onObject(preMapIds, newMapIds, 1, (int) tripleCount);
				mapper.onGraph(preMapIds, newMapIds, 1, (int) tripleCount);

				mapper.setShared(0);

				for (long id = 1; id <= tripleCount; id++) {
					assertEquals("subject mapping", id, mapper.extractSubject(id));
					assertEquals("predicate mapping", id, mapper.extractPredicate(id));
					assertEquals("object mapping", id, mapper.extractObjects(id));
					assertEquals("graph mapping", id, mapper.extractGraph(id));
				}

				mapper.delete();
			}
		}
	}
}

package com.the_qa_company.qendpoint.core.util;

import com.the_qa_company.qendpoint.core.dictionary.impl.section.PFCDictionarySection;
import com.the_qa_company.qendpoint.core.enums.RDFNodeType;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class SortedDictionarySectionIndexTest {
	@Parameterized.Parameters(name = "iri:{0} bnode:{1}")
	public static Collection<Object[]> params() {
		return List.of(new Object[][] { { true, true }, { false, true }, { true, false }, { false, false }, });
	}

	@Parameterized.Parameter
	public boolean withIRI;
	@Parameterized.Parameter(1)
	public boolean withBNode;

	@Test
	public void indexTest() {
		LargeFakeDataSetStreamSupplier supplier = LargeFakeDataSetStreamSupplier.createSupplierWithMaxTriples(999, 64)
				.withMaxElementSplit(20).withMaxLiteralSize(50).withIRI(withIRI).withBlankNode(withBNode);
		Iterator<TripleString> its = supplier.createTripleStringStream();

		Set<ByteString> elements = new HashSet<>();

		its.forEachRemaining(e -> elements.add(ByteString.of(e.getObject())));

		List<ByteString> strings = elements.stream().sorted().toList();
		PFCDictionarySection sec = new PFCDictionarySection(HDTOptions.empty());
		sec.load(strings.iterator(), strings.size(), ProgressListener.ignore());

		SortedDictionarySectionIndex index = new SortedDictionarySectionIndex(sec);

		Iterator<CharSequence> it = sec.getSortedEntries();
		long id = 1;
		while (it.hasNext()) {
			CharSequence bn = it.next();
			long bnid = id++;

			RDFNodeType excepted = RDFNodeType.typeof(bn);
			RDFNodeType actual = index.getNodeType(bnid);
			if (excepted != actual) {
				fail(format(
						"bnstart=%s[%d], bnodeend=%s-%s[%d]\nlitstart=%s[%d], litend=%s-%s[%d]\n%s!=%s for id %d %s->%s",
						sec.extract(index.bnodeStart), index.bnodeStart, sec.extract(index.bnodeEnd - 1),
						sec.extract(index.bnodeEnd), index.bnodeEnd, sec.extract(index.literalStart),
						index.literalStart, sec.extract(index.literalEnd - 1), sec.extract(index.literalEnd),
						index.literalEnd, excepted, actual, bnid, bn, sec.extract(bnid)));
			}
		}
	}
}

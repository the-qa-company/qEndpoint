package eu.qanswer.enpoint;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestNativeStore {

    private static final Logger logger = LoggerFactory.getLogger(TestNativeStore.class);

    public static void main(String[] args) {

        BitArrayDisk arrayDisk = new BitArrayDisk(0,"/Users/alyhdr/Desktop/qa-company/hdtsparqlendpoint/hdt-store/arr.bit");
        System.out.println(arrayDisk.access(100));

//        SPARQLRepository repository = new SPARQLRepository("https://query.wikidata.org/sparql");
//        IRI s = Values.iri("http://www.wikidata.org/entity/Q30600575");
//        IRI p = Values.iri("http://www.wikidata.org/prop/direct/P31");
//        IRI o = Values.iri("http://www.wikidata.org/entity/Q146");
//
//        RepositoryResult<Statement> statements = repository.getConnection().getStatements(null, p, o);
//        int count = 0;
//        while (statements.hasNext()) {
//            System.out.println(statements.next());
//            count++;
//        }
//        System.out.println(count);

    }

}

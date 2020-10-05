package eu.qanswer.functions;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A SPARQL function to change the format of dates
 *
 * @author Dennis Diefenbach
 */
public class ParseDateFunction implements Function {

    // define a constant for the namespace of our custom function
    public static final String NAMESPACE = "http://qanswer.eu/function/";

    /**
     * return the URI 'http://example.org/custom-function/palindrome' as a
     * String
     */
    public String getURI() {
        return NAMESPACE + "parse_date";
    }

    /**
     * Function to parse a date
     *
     * @return A boolean literal representing true if the input argument is a
     *         palindrome, false otherwise.
     * @throws ValueExprEvaluationException
     *         if more than one argument is supplied or if the supplied argument
     *         is not a literal.
     */
    public Value evaluate(ValueFactory valueFactory, Value... args)
            throws ValueExprEvaluationException
    {
        // our palindrome function expects only a single argument, so throw an error
        // if there's more than one
        if (args.length != 2) {
            throw new ValueExprEvaluationException(
                    "Parse date function requires"
                            + "exactly 2 argument, got "
                            + args.length);
        }
        Value arg1 = args[0];
        // check if the argument is a literal, if not, we throw an error
        if ((arg1 instanceof Literal)) {
            if (((Literal)arg1).getDatatype().equals("http://www.w3.org/2001/XMLSchema#date") || ((Literal)arg1).getDatatype().equals("http://www.w3.org/2001/XMLSchema#dateTime")){
                throw new ValueExprEvaluationException(
                        "invalid argument (expect an xsd:date or xsd:dateTime): " + arg1);
            }
        } else {
            throw new ValueExprEvaluationException(
                    "Invalid argument (expect an xsd:date or xsd:dateTime): " + arg1);
        }

        Value arg2 = args[1];
        if ((arg2 instanceof Literal)) {
            System.out.println(((Literal)arg1).getLabel());
            if (((Literal)arg1).getDatatype().toString().equals("http://www.w3.org/2001/XMLSchema#dateTime")){
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
                try {
                    Date date = simpleDateFormat.parse(((Literal)arg1).getLabel());
                    simpleDateFormat = new SimpleDateFormat(((Literal)arg2).getLabel());
                    return valueFactory.createLiteral(simpleDateFormat.format(date));
                } catch (ParseException e) {
                    throw new ValueExprEvaluationException(
                            "Cannot parse date: " + arg2);
                }
            }

        }

        // get the actual string value that we want to check for palindrome-ness.
        String label = ((Literal)arg1).getLabel();
        // we invert our string

        // a function is always expected to return a Value object, so we
        // return our boolean result as a Literal
        return valueFactory.createLiteral(label+"BLU");
    }
}

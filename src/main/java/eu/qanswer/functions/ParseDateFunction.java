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
     * return the URI 'http://qanswer.eu/function/parse_date' as a
     * String
     */
    public String getURI() {
        return NAMESPACE + "parse_date";
    }

    /**
     * Function to parse a date
     *
     * @return a string with the date formatted in the predefined format
     * @throws ValueExprEvaluationException
     *         if more than two argument is supplied or if the supplied argument
     *         is not a literal of the right type.
     */
    public Value evaluate(ValueFactory valueFactory, Value... args)
            throws ValueExprEvaluationException {
        if (args.length != 2) {
            throw new ValueExprEvaluationException(
                    "Parse date function requires"
                            + "exactly 2 argument, got "
                            + args.length);
        }
        Value arg1 = args[0];
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
            } else if (((Literal)arg1).getDatatype().toString().equals("http://www.w3.org/2001/XMLSchema#date")){
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    Date date = simpleDateFormat.parse(((Literal)arg1).getLabel());
                    simpleDateFormat = new SimpleDateFormat(((Literal)arg2).getLabel());
                    return valueFactory.createLiteral(simpleDateFormat.format(date));
                } catch (ParseException e) {
                    throw new ValueExprEvaluationException(
                            "Cannot parse date: " + arg2);
                }
            } else {
                throw new ValueExprEvaluationException(
                        "invalid argument (expect an xsd:date or xsd:dateTime): " + arg1);
            }

        } else {
            throw new ValueExprEvaluationException(
                    "Invalid argument (expect an xsd:date or xsd:dateTime): " + arg1);
        }
    }
}

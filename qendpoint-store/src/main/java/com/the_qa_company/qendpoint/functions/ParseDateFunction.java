package com.the_qa_company.qendpoint.functions;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

import java.text.SimpleDateFormat;

/**
 * A SPARQL function to change the format of dates
 *
 * @author Dennis Diefenbach
 */
public class ParseDateFunction implements Function {

	// define a constant for the namespace of our custom function
	public static final String NAMESPACE = "http://qanswer.eu/function/";
	public static final String URI = NAMESPACE + "parse_date";

	/**
	 * return the URI 'http://qanswer.eu/function/parse_date' as a String
	 */
	@Override
	public String getURI() {
		return URI;
	}

	/**
	 * Function to parse a date
	 *
	 * @return a string with the date formatted in the predefined format
	 * @throws ValueExprEvaluationException if more than two argument is
	 *                                      supplied or if the supplied argument
	 *                                      is not a literal of the right type.
	 */
	@Override
	public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {
		if (args.length != 2) {
			throw new ValueExprEvaluationException(
					"Parse date function requires" + "exactly 2 argument, got " + args.length);
		}
		Value arg1 = args[0];
		Value arg2 = args[1];
		if (arg1 instanceof Literal && arg2 instanceof Literal) {
			Literal date = (Literal) arg1;
			Literal format = (Literal) arg2;
			SimpleDateFormat formatter = new SimpleDateFormat(format.getLabel());
			String value = formatter.format(date.calendarValue().toGregorianCalendar().getTime());
			return valueFactory.createLiteral(value);
		} else {
			throw new ValueExprEvaluationException("Both arguments must be literals");
		}
	}
}

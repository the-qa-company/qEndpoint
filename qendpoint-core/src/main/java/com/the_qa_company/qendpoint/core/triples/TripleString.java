/*
 * File: $HeadURL:
 * https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/
 * triples/TripleString.java $ Revision: $Rev: 191 $ Last modified: $Date:
 * 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $ Last modified by: $Author:
 * mario.arias $ This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License,
 * or (at your option) any later version. This library is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin St,
 * Fifth Floor, Boston, MA 02110-1301 USA Contacting the authors: Mario Arias:
 * mario.arias@deri.org Javier D. Fernandez: jfergar@infor.uva.es Miguel A.
 * Martinez-Prieto: migumar2@infor.uva.es Alejandro Andres: fuzzy.alej@gmail.com
 */

package com.the_qa_company.qendpoint.core.triples;

import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.util.UnicodeEscape;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
import com.the_qa_company.qendpoint.core.util.string.CompactString;

import java.io.IOException;

/**
 * TripleString holds a triple as Strings
 */
public class TripleString {

	protected CharSequence subject;
	protected CharSequence predicate;
	protected CharSequence object;

	public TripleString() {
		this.subject = this.predicate = this.object = null;
	}

	/**
	 * Basic constructor
	 *
	 * @param subject   The subject
	 * @param predicate The predicate
	 * @param object    The object
	 */
	public TripleString(CharSequence subject, CharSequence predicate, CharSequence object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	/**
	 * Copy constructor
	 *
	 * @param other triple string to copy
	 */
	public TripleString(TripleString other) {
		this.subject = other.subject;
		this.predicate = other.predicate;
		this.object = other.object;
	}

	/**
	 * @return CharSequence the subject
	 */
	public CharSequence getSubject() {
		return subject;
	}

	/**
	 * @param subject the subject to set
	 */
	public void setSubject(CharSequence subject) {
		this.subject = subject;
	}

	/**
	 * @return CharSequence the predicate
	 */
	public CharSequence getPredicate() {
		return predicate;
	}

	/**
	 * @param predicate the predicate to set
	 */
	public void setPredicate(CharSequence predicate) {
		this.predicate = predicate;
	}

	/**
	 * @return the object
	 */
	public CharSequence getObject() {
		return object;
	}

	/**
	 * @param object the object to set
	 */
	public void setObject(CharSequence object) {
		this.object = object;
	}

	/**
	 * Sets all components at once. Useful to reuse existing object instead of
	 * creating new ones for performance.
	 *
	 * @param subject   subject
	 * @param predicate predicate
	 * @param object    object
	 */
	public void setAll(CharSequence subject, CharSequence predicate, CharSequence object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	protected boolean equalsCharSequence(CharSequence cs1, CharSequence cs2) {
		if (cs1 instanceof String && cs2 instanceof String)
			return cs1.equals(cs2); // use string method if we can

		if (cs1.length() != cs2.length())
			return false;

		for (int i = 0; i < cs1.length(); i++)
			if (cs1.charAt(i) != cs2.charAt(i))
				return false;
		return true;
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof TripleString ts && equalsCharSequence(subject, ts.subject)
				&& equalsCharSequence(predicate, ts.predicate) && equalsCharSequence(object, ts.object);
	}

	@Override
	public int hashCode() {
		// Same as Objects.hashCode(subject, predicate, object), with fewer
		// calls
		int s = subject == null ? 0 : subject.hashCode();
		int p = predicate == null ? 0 : predicate.hashCode();
		int o = object == null ? 0 : object.hashCode();
		return 31 * (31 * (31 * s) + p) + o;
	}

	/**
	 * Check whether this triple matches a pattern. A pattern is just a
	 * TripleString where each empty component means <em>any</em>.
	 *
	 * @param pattern triple pattern to search
	 * @return boolean
	 */
	public boolean match(TripleString pattern) {
		if (pattern.getSubject().length() == 0 || equalsCharSequence(pattern.getSubject(), this.subject)) {
			if (pattern.getPredicate().length() == 0 || equalsCharSequence(pattern.getPredicate(), this.predicate)) {
				return pattern.getObject().length() == 0 || equalsCharSequence(pattern.getObject(), this.object);
			}
		}
		return false;
	}

	/**
	 * Set all components to ""
	 */
	public void clear() {
		subject = predicate = object = "";
	}

	/**
	 * Checks whether all components are empty.
	 *
	 * @return boolean
	 */
	public boolean isEmpty() {
		return subject.length() == 0 && predicate.length() == 0 && object.length() == 0;
	}

	/**
	 * Checks whether any component is empty.
	 *
	 * @return boolean
	 */
	public boolean hasEmpty() {
		return subject.length() == 0 || predicate.length() == 0 || object.length() == 0;
	}

	/**
	 * Read from a line, where each component is separated by space.
	 *
	 * @param line line to read
	 * @throws ParserException if the line is not RDF complient
	 */
	public void read(String line) throws ParserException {
		read(line, false);
	}

	/**
	 * Read from a line, where each component is separated by space.
	 *
	 * @param line        line to read
	 * @param processQuad process quad
	 * @throws ParserException if the line is not RDF complient
	 */
	public void read(String line, boolean processQuad) throws ParserException {
		read(line, 0, line.length(), processQuad);
	}

	private int searchNextTabOrSpace(String line, int start, int end) {
		// searching space
		int sindex = line.indexOf(' ', start);
		if (sindex != -1 && sindex < end) {
			return sindex;
		}

		// not found, searching tabs
		int tindex = line.indexOf('\t', start);
		if (tindex != -1 && tindex < end) {
			return tindex;
		}

		// not found
		return -1;
	}

	private int searchBNodeBackward(String line, int start, int end) {
		// bn grammar
		// BLANK_NODE_LABEL ::= '_:' (PN_CHARS_U | [0-9]) ((PN_CHARS | '.')*
		// PN_CHARS)?
		// PN_CHARS_BASE ::= [A-Z] | [a-z] | [#x00C0-#x00D6] | [#x00D8-#x00F6] |
		// [#x00F8-#x02FF]
		// | [#x0370-#x037D] | [#x037F-#x1FFF] | [#x200C-#x200D] |
		// [#x2070-#x218F]
		// | [#x2C00-#x2FEF] | [#x3001-#xD7FF] | [#xF900-#xFDCF] |
		// [#xFDF0-#xFFFD]
		// | [#x10000-#xEFFFF]
		// PN_CHARS_U ::= PN_CHARS_BASE | '_' | ':'
		// PN_CHARS ::= PN_CHARS_U | '-' | [0-9] | #x00B7 | [#x0300-#x036F] |
		// [#x203F-#x2040]

		int loc = end;

		while (start > loc) {

			switch (line.charAt(loc)) {
			case ' ', '\t' -> {
				if (loc + 2 > end) {
					return -1;
				}
				if (line.charAt(loc + 1) == '_' && line.charAt(loc + 2) == ':') {
					return loc + 1;
				}
			}
			case '^', '@', '>', '<', '"' -> {
				// it wasn't a bnode
				return -1;
			}
			default -> {
				// ignore, we don't check the format
			}
			}
			loc--;
		}
		return -1;
	}

	/**
	 * Read from a line, where each component is separated by space.
	 *
	 * @param line  line to read
	 * @param start start in the string
	 * @param end   in the string
	 * @throws ParserException if the line is not RDF complient
	 */
	public void read(String line, int start, int end) throws ParserException {
		read(line, start, end, false);
	}

	/**
	 * Read from a line, where each component is separated by space.
	 *
	 * @param line        line to read
	 * @param start       start in the string
	 * @param end         in the string
	 * @param processQuad process quad
	 * @throws ParserException if the line is not RDF complient
	 */
	public void read(String line, int start, int end, boolean processQuad) throws ParserException {
		int split, posa, posb;
		// for quad implementation, don't forget to clear the graph
		this.clear();

		// SET SUBJECT
		posa = start;
		posb = split = searchNextTabOrSpace(line, posa, end);

		if (posb == -1) {
			// Not found, error.
			return;
		}
		if (line.charAt(posa) == '<') {
			posa++; // Remove <
			if (line.charAt(posb - 1) == '>') {
				posb--; // Remove >
			}
		}

		this.setSubject(UnicodeEscape.unescapeString(line, posa, posb));

		// SET PREDICATE
		posa = split + 1;
		posb = split = searchNextTabOrSpace(line, posa, end);

		if (posb == -1) {
			return;
		}
		if (line.charAt(posa) == '<') {
			posa++;
			if (posb > posa && line.charAt(posb - 1) == '>') {
				posb--;
			}
		}

		this.setPredicate(UnicodeEscape.unescapeString(line, posa, posb));

		if (processQuad) {
			// SET OBJECT
			posa = split + 1;
			posb = end;

			// Remove trailing <space> <dot> from NTRIPLES.
			if (line.charAt(posb - 1) == '.') {
				posb--;
			}
			char prev = line.charAt(posb - 1);
			if (prev == ' ' || prev == '\t') {
				posb--;
			}

			char lastElem = line.charAt(posb - 1);
			if (lastElem != '"') {
				if (lastElem == '>') {
					// can describe an IRI, can be:
					// datatype of a literal
					// object IRI
					// graph IRI

					int iriStart = line.lastIndexOf('<', posb);

					if (iriStart < posa) {
						throw new ParserException("end of a '>' without a start '<'", line, posb);
					}
					if (posa != iriStart && line.charAt(iriStart - 1) != '^') {
						this.setGraph(UnicodeEscape.unescapeString(line, iriStart + 1, posb - 1));
						posb = iriStart - 1;
					}
					// not the current element, literal or object iri
				} else {
					// end of a lang tag for a literal
					// end of an object BNode
					// end of a graph BNode

					// '_:' (PN_CHARS_U | [0-9]) ((PN_CHARS | '.')* PN_CHARS)?
					// PN_CHARS_U ::= PN_CHARS_BASE | '_' | ':'
					// PN_CHARS ::= PN_CHARS_U | '-' | [0-9] | #x00B7 |
					// [#x0300-#x036F] | [#x203F-#x2040]

					int bnodeStart = searchBNodeBackward(line, posa, posb);
					if (bnodeStart > posa) {
						this.setGraph(UnicodeEscape.unescapeString(line, bnodeStart + 1, posb - 1));
						posb = bnodeStart;
					}
					// not the current element, literal language or object bnode
				}
			}
			// a literal can't describe a graph
		} else {
			// SET OBJECT
			posa = split + 1;
			posb = end;

			// Remove trailing <space> <dot> from NTRIPLES.
			if (line.charAt(posb - 1) == '.') {
				posb--;
			}
			char prev = line.charAt(posb - 1);
			if (prev == ' ' || prev == '\t') {
				posb--;
			}
		}

		if (line.charAt(posa) == '<') {
			posa++;

			// Remove trailing > only if < appears, so "some"^^<http://datatype>
			// is kept as-is.
			if (posb > posa && line.charAt(posb - 1) == '>') {
				posb--;
			}
		}

		this.setObject(UnicodeEscape.unescapeString(line, posa, posb));
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return subject + " " + predicate + " " + object;
	}

	/**
	 * Convert TripleString to NTriple
	 *
	 * @return CharSequence
	 * @throws IOException when IOException occurs
	 */
	public CharSequence asNtriple() throws IOException {
		StringBuilder str = new StringBuilder();
		this.dumpNtriple(str);
		return str;
	}

	public final void dumpNtriple(Appendable out) throws IOException {
		char s0 = subject.charAt(0);
		if (s0 == '_' || s0 == '<') {
			out.append(subject);
		} else {
			out.append('<').append(subject).append('>');
		}

		char p0 = predicate.charAt(0);
		if (p0 == '<') {
			out.append(' ').append(predicate).append(' ');
		} else {
			out.append(" <").append(predicate).append("> ");
		}

		char o0 = object.charAt(0);
		if (o0 == '"') {
			UnicodeEscape.escapeString(object.toString(), out);
		} else if (o0 == '_' || o0 == '<') {
			out.append(object);
		} else {
			out.append('<').append(object).append(">");
		}

		CharSequence graph = getGraph();
		if (!graph.isEmpty()) {
			char g0 = graph.charAt(0);
			if (g0 == '<') {
				out.append(' ').append(graph);
			} else {
				out.append(" <").append(graph).append(">");
			}
		}

		out.append(" .\n");
	}

	/**
	 * convert all the elements into {@link String} and create a new
	 * TripleString
	 *
	 * @return tripleString
	 */
	public TripleString tripleToString() {
		return new TripleString(subject.toString(), predicate.toString(), object.toString());
	}

	/**
	 * copy all the elements using {@link ByteString#copy(CharSequence)} and
	 * create a new TripleString
	 *
	 * @return tripleString
	 */
	public TripleString tripleToByteString() {
		return new TripleString(ByteString.copy(subject), ByteString.copy(predicate), ByteString.copy(object));
	}

	/**
	 * copy all the elements using {@link ByteString#of(CharSequence)} and
	 * create a new TripleString
	 *
	 * @return tripleString
	 */
	public TripleString tripleToByteStringCast() {
		return new TripleString(ByteString.of(subject), ByteString.of(predicate), ByteString.of(object));
	}

	/**
	 * implementation for the graph context
	 *
	 * @param context context
	 */
	public void setGraph(CharSequence context) {
		// nothing
	}

	/**
	 * @return graph
	 */
	public CharSequence getGraph() {
		return "";
	}

	public boolean isStatic() {
		return !(subject == null || subject.isEmpty() || predicate == null || predicate.isEmpty() || object == null
				|| object.isEmpty());
	}
}

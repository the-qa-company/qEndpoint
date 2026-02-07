package org.apache.jena.iri.impl;

import java.io.Reader;

public class LexerFixer {

	private static final int CORES = Runtime.getRuntime().availableProcessors();

	public static void fixLexers() {
		Parser.lexers = new Lexer[CORES * 4][];
		for (int i = 0; i < Parser.lexers.length; i++) {
			Parser.lexers[i] = new Lexer[] { new LexerScheme((Reader) null), new LexerUserinfo((Reader) null),
					new LexerHost((Reader) null), new LexerPort((Reader) null), new LexerPath((Reader) null),
					new LexerQuery((Reader) null), new LexerFragment((Reader) null), new LexerXHost((Reader) null), };
		}
	}

	public static void printLexerSize() {
		int length = Parser.lexers.length;
		System.out.println("Lexer size: " + length);
	}
}

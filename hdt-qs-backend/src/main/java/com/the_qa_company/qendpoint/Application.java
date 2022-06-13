package com.the_qa_company.qendpoint;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.Arrays;

@SpringBootApplication
public class Application {
	public static void main(String[] args) {
		System.setProperty("org.eclipse.rdf4j.rio.verify_uri_syntax", "false");
		SpringApplicationBuilder bld = new SpringApplicationBuilder(Application.class);

		boolean client = Arrays.stream(args).anyMatch(arg -> arg.contains("--client"));
		if (client) {
			bld.headless(false);
		}

		bld.run(args);
	}
}

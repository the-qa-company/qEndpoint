package com.the_qa_company.qendpoint;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {
	public static void main(String[] args) {
		System.setProperty("org.eclipse.rdf4j.rio.verify_uri_syntax", "false");
		SpringApplication.run(Application.class, args);
	}
}

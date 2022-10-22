package com.the_qa_company.qendpoint;

import com.the_qa_company.qendpoint.controller.Sparql;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.solr.SolrAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;

@SpringBootApplication(exclude = { SolrAutoConfiguration.class })
public class Application {
	public static void main(String[] args) throws IOException, URISyntaxException {
		System.setProperty("org.eclipse.rdf4j.rio.verify_uri_syntax", "false");
		SpringApplicationBuilder bld = new SpringApplicationBuilder(Application.class);

		boolean client = Arrays.stream(args).anyMatch(arg -> arg.contains("--client"));

		// set default property for the client (to allow a bypass with
		// application.properties)
		if (client) {
			bld.headless(false);
			bld.properties("qendpoint.client=true");
		} else {
			bld.properties("qendpoint.client=false");
		}

		bld.registerShutdownHook(true);

		ConfigurableApplicationContext ctx = bld.run(args);

		// open the client
		Sparql sparql = ctx.getBean(Sparql.class);
		sparql.openClient();

		String s = "   qEndpoint started at: " + sparql.getServerAddress() + "   ";
		System.out.println("+" + "-".repeat(s.length()) + "+");
		System.out.println("|" + s + "|");
		System.out.println("+" + "-".repeat(s.length()) + "+");
	}
}

package com.the_qa_company.qendpoint.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebConfig implements WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> {
	@Autowired
	Sparql sparql;

	@Override
	public void customize(ConfigurableServletWebServerFactory factory) {
		// set the port asked by the user
		factory.setPort(sparql.getPort());
	}
}

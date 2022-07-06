package com.the_qa_company.qendpoint.config;

import com.the_qa_company.qendpoint.controller.Sparql;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

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

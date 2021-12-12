package com.the_qa_company.q_endpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    @Value("${server.port}")
    String port;

    public static void main(String[] args) {
        System.setProperty("org.eclipse.rdf4j.rio.verify_uri_syntax", "false");
        SpringApplication.run(Application.class, args);
    }
}

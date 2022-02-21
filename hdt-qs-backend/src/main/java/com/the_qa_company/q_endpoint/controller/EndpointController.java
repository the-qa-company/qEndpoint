package com.the_qa_company.q_endpoint.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;

@CrossOrigin(origins = "http://localhost:3001")
@RestController
@RequestMapping("/api/endpoint")
public class EndpointController {
    private static final Logger logger = LoggerFactory.getLogger(EndpointController.class);

    @Autowired
    private Sparql sparql;

    @RequestMapping(value = "/sparql")
    public ResponseEntity<String> sparqlEndpoint(
            @RequestParam(value = "query", required = false) final String query,
            @RequestParam(value = "update", required = false) final String updateQuery,
            @RequestParam(value = "format", defaultValue = "json") final String format,
            @RequestHeader(value = "Accept", defaultValue = "application/sparql-results+json") String acceptHeader,
            @RequestHeader(value = "timeout", defaultValue = "300") int timeout,
            @RequestHeader(value = "Content-Type", defaultValue = "text/plain") String content,

            @RequestBody(required = false) String body)
            throws Exception {
        logger.info("New query");
//        logger.info("Query {} timeout {} update query {} body {} ", query, timeout, updateQuery, body);

        if (query != null) {
            if (acceptHeader.contains("application/sparql-results+json")) {
                return ResponseEntity.status(HttpStatus.OK)
                        .header("Content-Type", "application/sparql-results+json")
                        .body(sparql.executeJson(query, timeout));
            }
            if (acceptHeader.contains("application/sparql-results+xml")) {
                return ResponseEntity.status(HttpStatus.OK)
                        .header("Content-Type", "application/sparql-results+xml")
                        .body(sparql.executeXML(query, timeout));
            }
            if (acceptHeader.contains("application/x-binary-rdf-results-table")) {
                return ResponseEntity.status(HttpStatus.OK)
                        .header("Content-Type", "application/x-binary-rdf-results-table")
                        .body(sparql.executeBinary(query, timeout));
            }
            if (format.equals("turtle") || acceptHeader.contains("text/turtle")) {
                return ResponseEntity.status(HttpStatus.OK)
                        .header("Content-Type", "application/sparql-results+json")
                        .body(sparql.executeTurtle(query, timeout));
            }
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Format not supported");

        } else {
            if (body != null && content.equals("application/sparql-query")) {
                return ResponseEntity.status(HttpStatus.OK)
                        .header("Content-Type", "application/sparql-results+json")
                        .body(sparql.executeJson(body, timeout));
            }
            if (updateQuery != null) {
                return ResponseEntity.status(HttpStatus.OK)
                        .body(sparql.executeUpdate(updateQuery, timeout));
            } else {
                if (body != null) {
                    return ResponseEntity.status(HttpStatus.OK)
                            .body(sparql.executeUpdate(body, timeout));
                }
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Query not specified");
            }
        }
    }

    @RequestMapping(value = "/update")
    public ResponseEntity<String> sparqlUpdate(
            @RequestParam(value = "query") final String query,
            @RequestParam(value = "format", defaultValue = "json") final String format,
            @RequestHeader(value = "Accept", defaultValue = "application/sparql-results+json") String acceptHeader,
            @RequestParam(value = "timeout", defaultValue = "5") int timeout,
            Principal principal)
            throws Exception {
        logger.info("Query " + query);
        logger.info("timeout: " + timeout);
        if (format.equals("json") || acceptHeader.contains("application/sparql-results+json")) {
            return ResponseEntity.status(HttpStatus.OK)
                    .header("Content-Type", "application/sparql-results+json")
                    .body(sparql.executeUpdate(query, timeout));
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Format not supported");
    }

    @PostMapping(value = "/load")
    public ResponseEntity<String> clearData(
            @RequestParam(value = "file") final MultipartFile file) {

        try {
            logger.info("Trying to index {} of size {}", file.getOriginalFilename(), file.getSize());
            InputStream inputStream = file.getInputStream();
            return sparql.loadFile(inputStream, file.getOriginalFilename(), file.getSize());
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("File was not loaded...\n");
        }
    }

}

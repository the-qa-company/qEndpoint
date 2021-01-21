package eu.qanswer.controller;

import eu.qanswer.enpoint.Sparql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;

@RestController
@RequestMapping("/api/endpoint")
public class EndpointController {
    private static final Logger logger = LoggerFactory.getLogger(EndpointController.class);

    @Autowired private Sparql sparql;
    @CrossOrigin(origins = "http://localhost:3000")
    @RequestMapping(value = "/sparql")
    public ResponseEntity<String> sparqlEndpoint(
            @RequestParam(value = "query", required = true) final String query,
            @RequestParam(value = "format", defaultValue = "json") final String format,
            @RequestHeader(value = "Accept", defaultValue = "application/sparql-results+json") String acceptHeader,
            @RequestParam(value = "timeout", defaultValue = "5") int timeout,
            Principal principal)
            throws Exception {
        logger.info("Query "+query);
        logger.info("timeout: "+timeout);
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

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Format not supported");
    }
    @RequestMapping(value = "/update")
    public ResponseEntity<String> sparqlUpdate(
            @RequestParam(value = "query", required = true) final String query,
            @RequestParam(value = "format", defaultValue = "json") final String format,
            @RequestHeader(value = "Accept", defaultValue = "application/sparql-results+json") String acceptHeader,
            @RequestParam(value = "timeout", defaultValue = "5") int timeout,
            Principal principal)
            throws Exception {
        logger.info("Query "+query);
        logger.info("timeout: "+timeout);
        if (format.equals("json") || acceptHeader.contains("application/sparql-results+json")) {
            return ResponseEntity.status(HttpStatus.OK)
                    .header("Content-Type", "application/sparql-results+json")
                    .body(sparql.executeUpdate(query, timeout));
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Format not supported");
    }
//    @RequestMapping(value = "/merge")
//    public ResponseEntity<String> makeMerge(
//            @RequestParam(value = "format", defaultValue = "json") final String format,
//            @RequestHeader(value = "Accept", defaultValue = "application/sparql-results+json") String acceptHeader,
//            Principal principal)
//            throws Exception {
//        if (format.equals("json") || acceptHeader.contains("application/sparql-results+json")) {
//            return ResponseEntity.status(HttpStatus.OK)
//                    .header("Content-Type", "application/sparql-results+json")
//                    .body(sparql.makeMerge());
//        }
//        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Format not supported");
//    }
    @RequestMapping(value = "/count")
    public ResponseEntity<String> getNativeCount(
            @RequestParam(value = "format", defaultValue = "json") final String format,
            @RequestHeader(value = "Accept", defaultValue = "application/sparql-results+json") String acceptHeader,
            Principal principal)
            throws Exception {
        if (format.equals("json") || acceptHeader.contains("application/sparql-results+json")) {
            return ResponseEntity.status(HttpStatus.OK)
                    .header("Content-Type", "application/sparql-results+json")
                    .body(sparql.getCurrentCount()+"");
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Format not supported");
    }

}

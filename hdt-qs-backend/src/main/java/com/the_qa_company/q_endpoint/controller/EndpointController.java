package com.the_qa_company.q_endpoint.controller;

import org.apache.tomcat.util.http.fileupload.MultipartStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.Principal;
import java.util.Arrays;
import java.util.Optional;

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
    private String extractBoundary(HttpServletRequest request) {
        String boundaryHeader = "boundary=";
        int i = request.getContentType().indexOf(boundaryHeader)+
                boundaryHeader.length();
        return request.getContentType().substring(i);
    }

    private String[][] readContentDispositionHeader(String header) {
        return Arrays.stream(header.split("\n|\r"))
                .filter(s -> !s.isEmpty())
                .filter(s -> s.startsWith("Content-Disposition: "))
                .map(s -> s.substring("Content-Disposition: ".length()))
                .flatMap(s -> Arrays.stream(s.split("; ")))
                .map(s -> s.split("=", 2))
                .toArray(String[][]::new);
    }

    @PostMapping(value = "/load", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<String> clearData(HttpServletRequest request) {
        String boundary = extractBoundary(request);
        try {
            MultipartStream multipartStream = new MultipartStream(request.getInputStream(),
                    boundary.getBytes(), 1024, null);
            boolean nextPart = multipartStream.skipPreamble();
            while(nextPart) {
                String header = multipartStream.readHeaders();
                String[][] cdHeader = readContentDispositionHeader(header);

                if (cdHeader.length == 0) {
                    nextPart = multipartStream.readBoundary();
                    continue; // not content-disposition header
                }

                String name = Arrays.stream(cdHeader)
                        .filter(s -> s[0].equals("name"))
                        .map(s -> s[1])
                        .map(s -> s.startsWith("\"") ? s.substring(1, s.length() - 1) : s)
                        .findFirst()
                        .orElse(null);

                if (name == null || !name.equals("file")){
                    nextPart = multipartStream.readBoundary();
                    continue; // not the file field
                }

                String filename = Arrays.stream(cdHeader)
                        .filter(s -> s[0].equals("filename"))
                        .map(s -> s[1])
                        .map(s -> s.startsWith("\"") ? s.substring(1, s.length() - 1) : s)
                        .findFirst()
                        .orElse(null);

                PipedInputStream pipedInputStream = new PipedInputStream();
                PipedOutputStream pipedOutputStream = new PipedOutputStream();
                pipedInputStream.connect(pipedOutputStream);
                Thread readingThread = new Thread(() -> {
                    try {
                        multipartStream.readBodyData(pipedOutputStream);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            pipedOutputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
                readingThread.start();

                logger.info("Trying to index {}", filename);
                ResponseEntity<String> out = sparql.loadFile(pipedInputStream, filename);
                try {
                    readingThread.join();
                } catch (InterruptedException e) {
                    //
                }
                return out;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("File was not loaded...\n");
    }

    @GetMapping("/merge")
    public ResponseEntity<Sparql.MergeRequestResult> mergeStore() throws Exception {
        return ResponseEntity.status(HttpStatus.OK).body(sparql.askForAMerge());
    }

    @GetMapping("/is_merging")
    public ResponseEntity<Sparql.IsMergingResult> isMerging() throws Exception {
        return ResponseEntity.status(HttpStatus.OK).body(sparql.isMerging());
    }
    @GetMapping("/")
    public ResponseEntity<String> home() {
        return ResponseEntity.status(HttpStatus.OK).body("ok");
    }
}

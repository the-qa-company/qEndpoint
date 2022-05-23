package com.the_qa_company.qendpoint.controller;

import org.apache.tomcat.util.http.fileupload.MultipartStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebInputException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

@CrossOrigin(origins = "http://localhost:3001")
@RestController
@RequestMapping("/api/endpoint")
public class EndpointController {
	private static final Logger logger = LoggerFactory.getLogger(EndpointController.class);

	@Autowired
	Sparql sparql;

	@RequestMapping(value = "/sparql")
	public void sparqlEndpoint(@RequestParam(value = "query", required = false) final String query,
			@RequestParam(value = "update", required = false) final String updateQuery,
			@RequestParam(value = "format", defaultValue = "json") final String format,
			@RequestHeader(value = "Accept", defaultValue = "application/sparql-results+json") String acceptHeader,
			@RequestHeader(value = "timeout", defaultValue = "500") int timeout,
			@RequestHeader(value = "Content-Type", defaultValue = "text/plain") String content,

			@RequestBody(required = false) String body, HttpServletResponse response) throws IOException {
		logger.info("New query");

		if (query != null) {
			sparql.execute(query, timeout, acceptHeader, response::setContentType, response.getOutputStream());
		} else if (body != null && content.equals("application/sparql-query")) {
			sparql.execute(body, timeout, acceptHeader, response::setContentType, response.getOutputStream());
		} else if (updateQuery != null) {
			sparql.executeUpdate(updateQuery, timeout, response.getOutputStream());
		} else if (body != null) {
			sparql.executeUpdate(body, timeout, response.getOutputStream());
		} else {
			throw new ServerWebInputException("Query not specified");
		}
	}

	@RequestMapping(value = "/update")
	public void sparqlUpdate(@RequestParam(value = "query") final String query,
			@RequestParam(value = "format", defaultValue = "json") final String format,
			@RequestHeader(value = "Accept", defaultValue = "application/sparql-results+json") String acceptHeader,
			@RequestParam(value = "timeout", defaultValue = "500") int timeout, HttpServletResponse response)
			throws IOException {
		logger.info("Query " + query);
		logger.info("timeout: " + timeout);
		if (format.equals("json")) {
			sparql.executeUpdate(query, timeout, response.getOutputStream());
		} else {
			throw new ServerWebInputException("Format not supported");
		}
	}

	private String extractBoundary(HttpServletRequest request) {
		String boundaryHeader = "boundary=";
		int i = request.getContentType().indexOf(boundaryHeader) + boundaryHeader.length();
		return request.getContentType().substring(i);
	}

	private String[][] readContentDispositionHeader(String header) {
		return Arrays.stream(header.split("[\n\r]")).filter(s -> !s.isEmpty())
				.filter(s -> s.startsWith("Content-Disposition: "))
				.map(s -> s.substring("Content-Disposition: ".length())).flatMap(s -> Arrays.stream(s.split("; ")))
				.map(s -> s.split("=", 2)).toArray(String[][]::new);
	}

	@PostMapping(value = "/load", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public ResponseEntity<Sparql.LoadFileResult> clearData(HttpServletRequest request) throws IOException {
		String boundary = extractBoundary(request);
		MultipartStream multipartStream = new MultipartStream(request.getInputStream(), boundary.getBytes(), 1024,
				null);
		boolean nextPart = multipartStream.skipPreamble();
		while (nextPart) {
			String header = multipartStream.readHeaders();
			String[][] cdHeader = readContentDispositionHeader(header);

			if (cdHeader.length == 0) {
				nextPart = multipartStream.readBoundary();
				continue; // not content-disposition header
			}

			String name = Arrays.stream(cdHeader).filter(s -> s[0].equals("name")).map(s -> s[1])
					.map(s -> s.startsWith("\"") ? s.substring(1, s.length() - 1) : s).findFirst().orElse(null);

			if (name == null || !name.equals("file")) {
				nextPart = multipartStream.readBoundary();
				continue; // not the file field
			}

			String filename = Arrays.stream(cdHeader).filter(s -> s[0].equals("filename")).map(s -> s[1])
					.map(s -> s.startsWith("\"") ? s.substring(1, s.length() - 1) : s).findFirst().orElse(null);

			PipedInputStream pipedInputStream = new PipedInputStream();
			PipedOutputStream pipedOutputStream = new PipedOutputStream();
			pipedInputStream.connect(pipedOutputStream);
			AtomicReference<IOException> exception = new AtomicReference<>(null);
			Thread readingThread = new Thread(() -> {
				try {
					multipartStream.readBodyData(pipedOutputStream);
				} catch (IOException e) {
					exception.set(e);
				} finally {
					try {
						pipedOutputStream.close();
					} catch (IOException e) {
						exception.set(e);
					}
				}
			}, "ClearDataReadThread");
			readingThread.start();

			logger.info("Trying to index {}", filename);
			Sparql.LoadFileResult out = sparql.loadFile(pipedInputStream, filename);
			try {
				readingThread.join();
			} catch (InterruptedException e) {
				//
			}
			// throw IOException of the read thread if required
			IOException e = exception.get();
			if (e != null) {
				throw e;
			}
			return ResponseEntity.status(HttpStatus.OK).body(out);
		}
		throw new IllegalArgumentException("no stream field");
	}

	@GetMapping("/merge")
	public ResponseEntity<Sparql.MergeRequestResult> mergeStore() {
		return ResponseEntity.status(HttpStatus.OK).body(sparql.askForAMerge());
	}

	@GetMapping("/reindex")
	public ResponseEntity<Sparql.LuceneIndexRequestResult> reindex() throws Exception {
		return ResponseEntity.status(HttpStatus.OK).body(sparql.reindexLucene());
	}

	@GetMapping("/is_merging")
	public ResponseEntity<Sparql.IsMergingResult> isMerging() {
		return ResponseEntity.status(HttpStatus.OK).body(sparql.isMerging());
	}

	@GetMapping("/")
	public ResponseEntity<String> home() {
		return ResponseEntity.status(HttpStatus.OK).body("ok");
	}
}

package com.the_qa_company.qendpoint.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/actuator")
public class LogController {
	@Autowired
	Sparql sparql;

	@GetMapping("/logfile")
	public void readLog(HttpServletResponse response) throws IOException {
		response.setContentType("text");
		sparql.writeLogs(response.getOutputStream());
	}
}

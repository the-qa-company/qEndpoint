package com.the_qa_company.q_endpoint;

import com.the_qa_company.q_endpoint.controller.Sparql;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.InputStream;

@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@SpringBootTest(classes = Application.class)
public class FileUploadTest {
    @Autowired
    Sparql sparql;

    private long fileSize(String file) {
        InputStream testNt = getClass().getClassLoader().getResourceAsStream(file);
        return 0;
    }

    @Test
    public void loadTest() {
        sparql.loadFile(getClass().getClassLoader().getResourceAsStream("cocktails.nt"), "cocktails.nt");

        sparql.debugMaxChunkSize = fileSize("cocktails.nt") / 10;
    }
}

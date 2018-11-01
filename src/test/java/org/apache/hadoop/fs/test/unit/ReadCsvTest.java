package org.apache.hadoop.fs.test.unit;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorInterface;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

public class ReadCsvTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(ReadCsvTest.class);
    
    static FileSystem fs;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        HcfsTestConnectorInterface connector = HcfsTestConnectorFactory.getHcfsTestConnector();
        fs = connector.create();
    }
    
    @Test
    public void smallCsv() throws IOException {
        Path p = new Path("/test/test1.csv");
        
        fs.delete(p, false);
        try {
            try (org.apache.hadoop.fs.FSDataOutputStream out = fs.create(p)) {
                IOUtils.copy(this.getClass().getResourceAsStream("/samples/sample.csv"), out);
            }
            int cnt = 0;
            try (Reader reader = new BufferedReader(new InputStreamReader(fs.open(p))); CSVReader csvReader = new CSVReader(reader);) {
                // Reading Records One by One in a String array
                String[] nextRecord;
                while ((nextRecord = csvReader.readNext()) != null) {
                    assertEquals("5 cols", 5, nextRecord.length);
                    assertEquals("Col 1 is C1", "C1", nextRecord[0]);
                    assertEquals("Col 5 is C5", "C5", nextRecord[4]);
                    LOG.info("C1 : " + nextRecord[0]);
                    LOG.info("C2 : " + nextRecord[1]);
                    LOG.info("C3 : " + nextRecord[2]);
                    LOG.info("C4 : " + nextRecord[3]);
                    LOG.info("C5 : " + nextRecord[4]);
                    LOG.info("==========================");
                    cnt++;
                }
            }
            assertEquals("5 records read", 5, cnt);
        } finally {
            fs.delete(p, false);
        }
    }
    
    @Test
    public void largeCsv() throws IOException {
        Path p = new Path("/test/test2.csv");
        
        fs.delete(p, false);
        try {
            try (org.apache.hadoop.fs.FSDataOutputStream out = fs.create(p)) {
                for (int i = 0; i < 100_000; i++) {
                    try (InputStream is = this.getClass().getResourceAsStream("/samples/sample.csv")) {
                        IOUtils.copy(is, out);
                    }
                }
            }
            int cnt = 0;
            try (Reader reader = new BufferedReader(new InputStreamReader(fs.open(p))); CSVReader csvReader = new CSVReader(reader);) {
                String[] nextRecord;
                while ((nextRecord = csvReader.readNext()) != null) {
                    assertEquals("5 cols", 5, nextRecord.length);
                    assertEquals("Col 1 is C1", "C1", nextRecord[0]);
                    assertEquals("Col 5 is C5", "C5", nextRecord[4]);
                    cnt++;
                }
            }
            assertEquals("500000 records read", 500000, cnt);
        } finally {
            fs.delete(p, false);
        }
    }
}

package org.apache.hadoop.fs.test.unit;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.orcl.OrclConfigKeys;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.junit.Test;

public class OrclFsDelegateTest {
    @Test
    public void test() throws IOException, URISyntaxException, RuntimeException {
        OrclConfigKeys c = new OrclConfigKeys();
        DelegateToFileSystem fs = HcfsTestConnectorFactory.getHcfsTestConnector().createDelegate();
        fs.getFsStatus(new Path("/"));
        //fs.getFsStatus();
    }
    
}

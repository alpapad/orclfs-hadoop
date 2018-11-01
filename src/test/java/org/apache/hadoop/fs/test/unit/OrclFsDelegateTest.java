package org.apache.hadoop.fs.test.unit;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.orcl.OrclConfigKeys;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.junit.Test;

public class OrclFsDelegateTest {
    @Test
    public void test() throws IOException, URISyntaxException, RuntimeException {
        OrclConfigKeys c = new OrclConfigKeys();
        assertNotNull(c);
        
        DelegateToFileSystem fs = HcfsTestConnectorFactory.getHcfsTestConnector().createDelegate();
        assertNotNull(fs);
    }
    
    @Test
    public void testGetFsStatusWithPath() throws IOException, URISyntaxException, RuntimeException {
        DelegateToFileSystem fs = HcfsTestConnectorFactory.getHcfsTestConnector().createDelegate();
        FsStatus stat = fs.getFsStatus(new Path("/"));
        assertNotNull(stat);
    }
    
    @Test
    public void testGetFsStatus() throws IOException, URISyntaxException, RuntimeException {
        DelegateToFileSystem fs = HcfsTestConnectorFactory.getHcfsTestConnector().createDelegate();
        FsStatus stat = fs.getFsStatus();
        assertNotNull(stat);
    }
}

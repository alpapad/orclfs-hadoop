package org.apache.hadoop.fs.orcl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

public class OrclFs extends DelegateToFileSystem {
    
    public OrclFs(final URI theUri, final Configuration conf) throws IOException, URISyntaxException {
        super(theUri, new OrclFileSystem(conf), conf, "orcl", true);
    }
}

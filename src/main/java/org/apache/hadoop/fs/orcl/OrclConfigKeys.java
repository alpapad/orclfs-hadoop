package org.apache.hadoop.fs.orcl;

import org.apache.hadoop.fs.CommonConfigurationKeys;

public class OrclConfigKeys extends CommonConfigurationKeys {
    
    public static final long ORCL_BLOCK_SIZE_DEFAULT = 64 * 1024 * 1024;
    public static final String ORCL_BLOCK_SIZE_KEY = "orcl.object.size";
    
    public static final String ORCL_PORT = "orcl.port";
    public static final int ORCL_PORT_DEFAULT = 1521;
    
}

package org.apache.hadoop.fs.orcl;

import org.apache.hadoop.fs.CommonConfigurationKeys;

public class OrclConfigKeys extends CommonConfigurationKeys {
    
    public static final long ORCL_BLOCK_SIZE_DEFAULT = 64 * 1024 * 1024;
    public static final String ORCL_BLOCK_SIZE_KEY = "orclfs.object.size";
    
    public static final String ORCL_PORT = "orclfs.port";
    public static final int ORCL_PORT_DEFAULT = 1521;
    
    public static final String ORCL_CONNECTION_FACTORY_CLASS = "orclfs.connection.factory.class-name";
    public static final String ORCL_CONNECTION_FACTORY_CLASS_DEFAULT = "oracle.jdbc.pool.OracleDataSource";
    
    public static final String ORCL_CONNECTION_URL = "orclfs.connection.url";
    public static final String ORCL_CONNECTION_USER = "orclfs.connection.user";
    
    public static final String ORCL_CONNECTION_PASSWORD = "orclfs.connection.password";
    
    public static final String ORCL_CONNECTION_VALIDATION_QUERY = "orclfs.connection.validation-query";
    public static final String ORCL_CONNECTION_VALIDATION_QUERY_DEFAULT = "select 1 from dual";

    public static final String ORCL_CONNECTION_INITIAL_POOL_SIZE = "orclfs.connection.initial-pol-size";
    public static final int ORCL_CONNECTION_INITIAL_POOL_SIZE_DEFAULT = 5;
    
    protected static final String ORCL_CONNECTION_MIN_POOL_SIZE = "orclfs.connection.min-pol-size";
    protected static final int ORCL_CONNECTION_MIN_POOL_SIZE_DEFAULT = 5;
    
    protected static final String ORCL_CONNECTION_MAX_POOL_SIZE = "orclfs.connection.max-pol-size";
    protected static final int ORCL_CONNECTION_MAX_POOL_SIZE_DEFAULT = 10;
    
    protected static final String ORCL_CONNECTION_VALIDATE_ON_BORROW = "orclfs.connection.validate-on-borrow";
    protected static final boolean ORCL_CONNECTION_VALIDATE_ON_BORROW_DEFAULT = true;
}

package org.apache.hadoop.fs.orcl;

import java.io.IOException;
import java.sql.SQLException;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;

@FunctionalInterface
public interface DoWithStatement<T> {
    T execute(OracleConnection con, OraclePreparedStatement st) throws IOException, SQLException;
}

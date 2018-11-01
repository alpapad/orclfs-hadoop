/*
 * 
 */
package org.apache.hadoop.fs.orcl;

import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_FACTORY_CLASS;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_FACTORY_CLASS_DEFAULT;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_INITIAL_POOL_SIZE;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_INITIAL_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_MAX_POOL_SIZE;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_MAX_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_MIN_POOL_SIZE;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_MIN_POOL_SIZE_DEFAULT;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_URL;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_USER;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_VALIDATE_ON_BORROW;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_VALIDATE_ON_BORROW_DEFAULT;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_VALIDATION_QUERY;
import static org.apache.hadoop.fs.orcl.OrclConfigKeys.ORCL_CONNECTION_VALIDATION_QUERY_DEFAULT;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

class OrclClientImpl extends OrclFsClient {
    private static class CacheKey {
        Configuration configuration;
        URI uri;
        
        public CacheKey(URI uri, Configuration configuration) {
            super();
            this.uri = uri;
            this.configuration = configuration;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((uri == null) ? 0 : uri.hashCode());
            return result;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            CacheKey other = (CacheKey) obj;
            if (uri == null) {
                if (other.uri != null)
                    return false;
            } else if (!uri.equals(other.uri))
                return false;
            return true;
        }
        
        public String getStr(String key, String defaultValue) throws SQLException {
            return configuration.get(key, defaultValue).trim();
        }
        
        public String getStr(String key) throws SQLException {
            String value = configuration.get(key);
            if (value == null || value.trim().isEmpty()) {
                throw new SQLException("ORCLFS property '" + key + "' is is required but not set");
            }
            
            return value.trim();
        }
        
        public int getInt(String key, int defaultValue) {
            return configuration.getInt(key, defaultValue);
        }
        
        public boolean getBool(String key, boolean defaultValue) {
            return configuration.getBoolean(key, defaultValue);
        }
    }
    
    private final static LoadingCache<CacheKey, PoolDataSource> pdsCache = CacheBuilder.newBuilder().maximumSize(1000).expireAfterWrite(10, TimeUnit.MINUTES)
            .build(new CacheLoader<CacheKey, PoolDataSource>() {
                public PoolDataSource load(CacheKey key) throws SQLException {
                    PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
                    // Setting connection properties of the data source
                    pds.setConnectionFactoryClassName(key.getStr(ORCL_CONNECTION_FACTORY_CLASS, ORCL_CONNECTION_FACTORY_CLASS_DEFAULT));
                    pds.setURL(key.getStr(ORCL_CONNECTION_URL));
                    pds.setUser(key.getStr(ORCL_CONNECTION_USER));
                    pds.setPassword(key.getStr(OrclConfigKeys.ORCL_CONNECTION_PASSWORD));
                    
                    // Setting pool properties
                    pds.setInitialPoolSize(key.getInt(ORCL_CONNECTION_INITIAL_POOL_SIZE, ORCL_CONNECTION_INITIAL_POOL_SIZE_DEFAULT));
                    pds.setMinPoolSize(key.getInt(ORCL_CONNECTION_MIN_POOL_SIZE, ORCL_CONNECTION_MIN_POOL_SIZE_DEFAULT));
                    pds.setMaxPoolSize(key.getInt(ORCL_CONNECTION_MAX_POOL_SIZE, ORCL_CONNECTION_MAX_POOL_SIZE_DEFAULT));
                    pds.setValidateConnectionOnBorrow(key.getBool(ORCL_CONNECTION_VALIDATE_ON_BORROW, ORCL_CONNECTION_VALIDATE_ON_BORROW_DEFAULT));
                    pds.setSQLForValidateConnection(key.getStr(ORCL_CONNECTION_VALIDATION_QUERY, ORCL_CONNECTION_VALIDATION_QUERY_DEFAULT));
                    return pds;
                }
            });
    
    private Configuration configuration;
    private URI uri;
    private PoolDataSource pds;
    private CacheKey key;
    
    public OrclClientImpl() {
    }
    
    @Override
    void initialize(URI uri, Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.key = new CacheKey(this.uri, this.configuration);
        try {
            this.pds = pdsCache.get(this.key);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }
    
    OrclInode __open(Path path, int flags, short mode) throws IOException {
        OrclInode inode = fetchInodeInfo(path);
        if (inode == null) {
            if ((flags & OrclInode.O_CREAT) == OrclInode.O_CREAT) {
                Path parent = path.getParent();
                this.mkdirs(parent, FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(configuration)).toShort());
                OrclInode pInode = fetchInodeInfo(parent);
                inode = createEntry(pInode, path.getName(), OrclInode.FILE_TYPE, mode);
                if (inode == null) {
                    throw new IOException("Could not create file... " + path);
                }
                createDataNode(inode);
                return inode;
            }
        } else {
            if ((flags & OrclInode.O_TRUNC) == OrclInode.O_TRUNC) {
                truncFile(inode);
            }
        }
        return inode;
    }
    
    @Override
    void chmod(Path path, short mode) throws IOException {
        OrclInode inode = fetchInodeInfo(path);
        if (inode != null) {
            this.execute("UPDATE FS_INODE SET IMODE = ? WHERE ID = ?", (con, st) -> {
                st.setShort(1, mode);
                st.setLong(2, inode.getId());
                st.executeUpdate();
                con.commit();
                return null;
            });
        } else {
            throw new PathNotFoundException(pathString(path));
        }
    }
    
    @Override
    OrclInode[] listdir(Path path) throws IOException {
        OrclInode inode = fetchInodeInfo(path);
        if (inode != null) {
            return this.execute("SELECT ID, PARENT, NAME, PATH, ITYPE, IMODE, IOWNER, IGROUP, ISIZE, A_TIME, M_TIME FROM VFS_INODE WHERE PARENT = ?", (con, st) -> {
                List<OrclInode> inodes = new ArrayList<>();
                st.setLong(1, inode.getId());
                try (ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        inodes.add(map(rs));
                    }
                }
                return inodes.toArray(new OrclInode[inodes.size()]);
            });
        } else {
            throw new FileNotFoundException(pathString(path));
        }
    }
    
    @Override
    OrclInode lstat(Path path) throws IOException {
        return fetchInodeInfo(path);
    }
    
    @Override
    OrclInode open(Path path, int flags, short mode) throws IOException {
        OrclInode stat = __open(path, flags, mode);
        if (stat.isDir()) {
            throw new FileNotFoundException(path.toString());
        }
        return stat;
    }
    
    @Override
    boolean mkdirs(Path path, short mode) throws IOException {
        boolean created = true;
        Path absolute = path;
        OrclInode inode = fetchInodeInfo(absolute);
        String pathName = absolute.getName();
        if (inode == null) {
            Path parent = absolute.getParent();
            created = parent == null || mkdirs(parent, mode);
            if (created) {
                String parentDir;
                if (parent == null) {
                    parentDir = "/";
                } else {
                    parentDir = parent.toUri().getPath();
                }
                created = created && makeDirectory(parentDir, pathName, mode);
            }
        } else if (inode.isFile()) {
            throw new ParentNotDirectoryException(String.format("Can't make directory for path %s since it is a file.", absolute));
        }
        return created;
    }
    
    @Override
    int read(OrclInode inode, long pos, byte[] buffer, int length) throws IOException {
        return this.execute("SELECT BLOCK_DATA FROM FS_BLOCK WHERE ID = ?", (con, st) -> {
            st.setLong(1, inode.getId());
            try (ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    Blob b = rs.getBlob(1);
                    long len = Math.min(length, b.length() - pos);
                    if (pos >= b.length() || len < 0) {
                        return 0;
                    }
                    try (InputStream is = b.getBinaryStream(pos + 1, len)) {
                        return is.read(buffer);
                    }
                }
            }
            return -1;
        });
    }
    
    @Override
    void rename(Path src, Path dst) throws IOException {
        OrclInode srcInode = fetchInodeInfo(src);
        if (srcInode == null) {
            throw new PathNotFoundException("Source path " + src + " does not exist");
        }
        if (isParentOf(src, dst)) {
            throw new IOException("Cannot rename " + src + " under itself" + " : " + dst);
        }
        
        OrclInode dstInode = fetchInodeInfo(dst);
        
        // Destination exists
        if (dstInode != null) {
            if (!dstInode.isDir()) {
                throw new PathIsNotDirectoryException("Path " + dst);
            }
            
            if (exists(new Path(dst, src.getName()))) {
                throw new PathExistsException("Destination path " + dst + " already exists");
            }
            
            moveTo(srcInode, dst);
            return;
        } else {
            mkdirs(dst.getParent(), FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(configuration)).toShort());
            copyTo(srcInode, dst.getParent(), dst.getName());
        }
    }
    
    @Override
    void rmdir(Path path) throws IOException {
        if (path.isRoot()) {
            // we dont delete the root path...
            return;
        }
        OrclInode inode = this.fetchInodeInfo(path);
        if (inode != null) {
            assert inode.isDir();
            deleteInode(inode);
        }
    }
    
    @Override
    protected void setTimes(Path path, long atime, long mtime) throws IOException {
        OrclInode inode = fetchInodeInfo(path);
        if (inode != null) {
            this.execute("UPDATE FS_INODE SET A_TIME = ?, M_TIME = ? WHERE ID = ?", (con, st) -> {
                st.setLong(1, atime);
                st.setLong(2, mtime);
                st.setLong(3, inode.getId());
                st.executeUpdate();
                con.commit();
                return null;
            });
        } else {
            throw new FileNotFoundException(pathString(path));
        }
        
    }
    
    @Override
    void close() throws IOException {
        this.pds = null;
        this.configuration = null;
        this.uri = null;
        this.key = null;
    }
    
    @Override
    FsStatus statfs(Path p) {
        FsStatus st = new FsStatus(Long.MAX_VALUE, 0, Long.MAX_VALUE);
        return st;
    }
    
    @Override
    void unlink(Path path) throws IOException {
        OrclInode inode = this.fetchInodeInfo(path);
        assert inode.isFile();
        dropContents(inode);
        deleteInode(inode);
    }
    
 // See: https://vsadilovskiy.wordpress.com/2007/11/19/blob-write-size-and-cpu/
    private final static int writeSize = 32 * 1024 - 240;
    
    @Override
    int write(OrclInode inode, final byte[] buffer, final int size) throws IOException {
        return this.selectForUpdate("SELECT BLOCK_DATA FROM FS_BLOCK WHERE ID = ? FOR UPDATE", (con, st) -> {
            st.setLong(1, inode.getId());
            try (ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    Blob b = rs.getBlob(1);
                    
                    try (final BufferedOutputStream out = new BufferedOutputStream(b.setBinaryStream(b.length() + 1))) {
                        if (size < writeSize) {
                            out.write(buffer, 0, size);
                        } else {
                            int ofs = 0;
                            
                            while (ofs < size) {
                                int l = Math.min(writeSize, size - ofs);
                                out.write(buffer, ofs, l);
                                out.flush();
                                ofs += l;
                            }
                        }
                    }
                    con.commit();
                    return size;
                }
            }
            return -1;
        });
    }
    
    private void copyTo(OrclInode sourceInode, Path destinationPath, String targetName) throws IOException {
        OrclInode dstInode = fetchInodeInfo(destinationPath);
        this.execute("UPDATE FS_INODE SET PARENT = ?, NAME= ? WHERE ID = ?", (con, st) -> {
            st.setLong(1, dstInode.getId());
            st.setString(2, targetName);
            st.setLong(3, sourceInode.getId());
            int updated = st.executeUpdate();
            assert updated == 1;
            con.commit();
            return null;
        });
    }
    
    private OrclInode createEntry(OrclInode parentInode, String name, int type, short mode) throws IOException {
        String cols[] = { "ID", "PARENT", "NAME", "ITYPE", "IMODE", "IOWNER", "IGROUP", "ISIZE", "A_TIME", "M_TIME" };
        final String owner = System.getProperties().getProperty("user.name");
        final String group = System.getProperties().getProperty("user.name");
        long id = -1;
        OrclInode inode = null;
        
        try (OracleConnection con = getConnection()) {
            long ts = new Date().getTime();
            try (OraclePreparedStatement st = prep(con, "INSERT INTO FS_INODE (ID, PARENT, NAME, ITYPE, IMODE, IOWNER, IGROUP, ISIZE, A_TIME, M_TIME) VALUES (FS_INODE_SEQ.NEXTVAL,?,?,?,?,?,?,0,?, ?)",
                    cols)) {
                st.setLong(1, parentInode.getId());
                st.setString(2, name.trim());
                st.setInt(3, type);
                st.setShort(4, mode);
                st.setString(5, owner);
                st.setString(6, group);
                st.setLong(7, ts);
                st.setLong(8, ts);
                st.executeUpdate();
                ResultSet rset = st.getGeneratedKeys();
                if (rset.next()) {
                    id = rset.getLong(1);
                }
                con.commit();
                String parent = parentInode.getPath();
                String absPath;
                if (parent == null || parent.equals("/")) {
                    absPath = "/" + name;
                } else {
                    absPath = parent + "/" + name;
                }
                inode = new OrclInode(id, parentInode.getId(), name, absPath, type, mode, owner, group, 0, ts, ts);
                return inode;
            }
        } catch (SQLException e) {
            return inode;
        }
    }
    
    void truncFile(OrclInode inode) throws IOException {
        this.execute("UPDATE FS_BLOCK  SET BLOCK_DATA = EMPTY_BLOB() WHERE ID = ?", (con, st) -> {
            st.setLong(1, inode.getId());
            st.executeUpdate();
            con.commit();
            return null;
        });
    }
    
    private void createDataNode(OrclInode inode) throws IOException {
        this.execute("INSERT INTO FS_BLOCK (ID, BLOCK_DATA) VALUES(?, EMPTY_BLOB())", (con, st) -> {
            st.setLong(1, inode.getId());
            st.executeUpdate();
            con.commit();
            return null;
        });
    }
    
    private void deleteInode(OrclInode inode) throws IOException {
        assert inode != null;
        assert inode.getId() != 0;
        this.execute("DELETE FROM FS_INODE WHERE ID = ?", (con, st) -> {
            st.setLong(1, inode.getId());
            st.executeUpdate();
            con.commit();
            return null;
        });
    }
    
    private void dropContents(OrclInode inode) throws IOException {
        this.execute("DELETE FROM FS_BLOCK WHERE ID = ?", (con, st) -> {
            st.setLong(1, inode.getId());
            st.executeUpdate();
            con.commit();
            return null;
        });
    }
    
    private boolean exists(Path path) {
        try {
            if (lstat(path) != null) {
                return true;
            }
        } catch (IOException e) {
            
        }
        return false;
    }
    
    private OrclInode fetchInodeInfo(Path path) throws IOException {
        return fetchInodeInfo(pathString(path));
    }
    
    private OrclInode fetchInodeInfo(String path) throws IOException {
        return this.execute("SELECT ID, PARENT, NAME, PATH, ITYPE, IMODE, IOWNER, IGROUP, ISIZE, A_TIME, M_TIME FROM VFS_INODE WHERE PATH = ?", (con, st) -> {
            st.setString(1, path);
            try (ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    return map(rs);
                }
            }
            return null;
        });
    }
    
    private boolean isParentOf(Path parent, Path child) {
        URI parentURI = parent.toUri();
        String parentPath = parentURI.getPath();
        if (!parentPath.endsWith("/")) {
            parentPath += "/";
        }
        URI childURI = child.toUri();
        String childPath = childURI.getPath();
        return childPath.startsWith(parentPath);
    }
    
    private boolean makeDirectory(String parentDir, String pathName, short perm) throws IOException {
        OrclInode inode = fetchInodeInfo(parentDir);
        assert inode.isDir();
        return createEntry(inode, pathName, OrclInode.DIRECTORY_TYPE, perm) != null;
    }
    
    private OrclInode map(ResultSet rs) throws SQLException {
        long id = rs.getLong("ID");
        Long parent = rs.getLong("PARENT");
        if (rs.wasNull()) {
            parent = null;
        }
        String name = rs.getString("NAME");
        if (rs.wasNull()) {
            name = "";
        }
        String path = rs.getString("PATH");
        int type = rs.getInt("ITYPE");
        short mode = rs.getShort("IMODE");
        String owner = rs.getString("IOWNER");
        String group = rs.getString("IGROUP");
        long size = rs.getLong("ISIZE");
        long atime = rs.getLong("A_TIME");
        long mtime = rs.getLong("M_TIME");
        return new OrclInode(id, parent, name, path, type, mode, owner, group, size, atime, mtime);
    }
    
    private void moveTo(OrclInode srcInode, Path dst) throws IOException {
        final OrclInode dstInode = fetchInodeInfo(dst);
        
        this.execute("UPDATE FS_INODE SET PARENT = ? WHERE ID = ?", (con, st) -> {
            st.setLong(1, dstInode.getId());
            st.setLong(2, srcInode.getId());
            int updated = st.executeUpdate();
            assert updated == 1;
            con.commit();
            return null;
        });
    }
    
    private String pathString(Path path) {
        String p = path.toUri().getPath();
        if (p.endsWith("/") && p.length() > 1) {
            return p.substring(0, p.length() - 1);
        }
        return p;
    }
    
    private OracleConnection getConnection() throws SQLException {
        OracleConnection conn = OracleConnection.class.cast(pds.getConnection());
        conn.setAutoCommit(false);
        return conn;
    }
    
    private OraclePreparedStatement prep(OracleConnection con, String sql, String... columnNames) throws SQLException {
        if (columnNames != null && columnNames.length != 0) {
            return OraclePreparedStatement.class.cast(con.prepareStatement(sql, columnNames));
        } else {
            return OraclePreparedStatement.class.cast(con.prepareStatement(sql));
        }
    }
    
    private <T> T execute(String sql, DoWithStatement<T> exec) throws IOException {
        try (OracleConnection con = getConnection()) {
            try (OraclePreparedStatement st = prep(con, sql)) {
                return exec.execute(con, st);
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
    
    private <T> T selectForUpdate(String sql, DoWithStatement<T> exec) throws IOException {
        try (OracleConnection con = getConnection()) {
            try (OraclePreparedStatement st = OraclePreparedStatement.class.cast(con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE))) {
                return exec.execute(con, st);
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}

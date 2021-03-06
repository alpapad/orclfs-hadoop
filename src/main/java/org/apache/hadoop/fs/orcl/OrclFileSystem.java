package org.apache.hadoop.fs.orcl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class OrclFileSystem extends FileSystem {
    
    private OrclFsClient client = null;
    private URI uri;
    private Path workingDir;
    
    public OrclFileSystem() {
    }
    
    public OrclFileSystem(Configuration conf) {
        // BAD BAD BAD BAD
        this.setConf(conf);
    }
    
    @Override
    public String getScheme() {
        return "orcl";
    }
    
    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        
        progress(progress);
        
        OrclInode inode = client.lstat(path);
        
        progress(progress);
        
        if (inode == null || !inode.isFile()) {
            throw new FileNotFoundException("Path " + path + " does not exist or it not a file");
        }
        OrclOutputStream ostream = new OrclOutputStream(getConf(), client, inode, bufferSize);
        return new FSDataOutputStream(ostream, statistics, inode.getSize());
    }
    
    /*
     * Concat existing files together.
     * 
     * @param trg the path to the target destination.
     * 
     * @param psrcs the paths to the sources to use for the concatenation.
     * 
     * @throws IOException IO failure
     * 
     * @throws UnsupportedOperationException if the operation is unsupported
     * (default).
     *
     * public void concat(final Path trg, final Path [] psrcs) throws IOException {
     * throw new UnsupportedOperationException("Not implemented by the " +
     * getClass().getSimpleName() + " FileSystem implementation"); }
     */
    
    @Override
    public void close() throws IOException {
        super.close();
        client.close();
    }
    
    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        
        progress(progress);
        
        OrclInode inode = client.lstat(path);
        
        progress(progress);
        
        boolean exists = inode != null;
        if (exists && !inode.isFile()) {
            throw new FileAlreadyExistsException("Path is a directory..." + path);
        }
        
        progress(progress);
        
        int flags = OrclInode.O_WRONLY | OrclInode.O_CREAT;
        
        if (exists) {
            if (overwrite) {
                flags |= OrclInode.O_TRUNC;
            } else {
                throw new FileAlreadyExistsException("Path exists:" + path);
            }
        } else {
            Path parent = path.getParent();
            if (parent != null) {
                if (!mkdirs(parent)) {
                    throw new IOException("mkdirs failed for " + parent.toString());
                }
            }
        }
        
        progress(progress);
        
        inode = client.open(path, flags, permission.toShort());
        
        progress(progress);
        
        OrclOutputStream ostream = new OrclOutputStream(getConf(), client, inode, bufferSize);
        return new FSDataOutputStream(ostream, statistics);
    }
    
    private static void progress(Progressable progress) {
        if (progress != null) {
            progress.progress();
        }
    }
    
    @Override
    public FSDataOutputStream createNonRecursive(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        
        Path parent = path.getParent();
        
        if (parent != null) {
            OrclInode stat = client.lstat(path);
            if (stat != null) {
                if (stat.isFile()) {
                    if (!overwrite) {
                        throw new FileAlreadyExistsException(path.toString());
                    }
                } else {
                    throw new FileAlreadyExistsException(path.toString());
                }
            }
        }
        
        return this.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    }
    
    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException {
        if (exists(f) && !flags.contains(CreateFlag.OVERWRITE)) {
            throw new FileAlreadyExistsException("File already exists: " + f);
        }
        return this.createNonRecursive(f, permission, flags.contains(CreateFlag.OVERWRITE), bufferSize, replication, blockSize, progress);
    }
    
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        /* path exists? */
        FileStatus status;
        try {
            status = getFileStatus(path);
        } catch (FileNotFoundException e) {
            return false;
        }
        
        if (status.isFile()) {
            client.unlink(path);
            return true;
        }
        
        FileStatus[] dirlist = listStatus(path);
        
        if (!recursive && dirlist.length > 0) {
            throw new PathIsNotEmptyDirectoryException("Directory " + path.toString() + "is not empty.");
        }
        
        for (FileStatus fs : dirlist) {
            if (!delete(fs.getPath(), recursive)) {
                return false;
            }
        }
        client.rmdir(path);
        return true;
    }
    
    @Override
    public long getDefaultBlockSize() {
        return getConf().getLong(OrclConfigKeys.ORCL_BLOCK_SIZE_KEY, OrclConfigKeys.ORCL_BLOCK_SIZE_DEFAULT);
    }
    
    @Override
    protected int getDefaultPort() {
        return getConf().getInt(OrclConfigKeys.ORCL_PORT, OrclConfigKeys.ORCL_PORT_DEFAULT);
    }
    
    @Override
    public short getDefaultReplication() {
        return 1;
    }
    
    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        
        OrclInode inode = client.lstat(path);
        if (inode == null) {
            throw new FileNotFoundException("" + path);
        }
        FileStatus status = new FileStatus(inode.getSize(), //
                inode.isDir(), //
                getDefaultReplication(), //
                getDefaultBlockSize(), //
                inode.getAtime(), //
                inode.getAtime(), //
                new FsPermission(inode.getMode()), inode.getOwner(), //
                inode.getGroup(), //
                path.makeQualified(getUri(), getWorkingDirectory()));
        
        return status;
    }
    
    @Override
    public FsStatus getStatus(Path p) throws IOException {
        if(p == null) {
            p = new Path("/");
        }
        checkPath(p);
        return client.statfs(p);
    }
    
    @Override
    public URI getUri() {
        return uri;
    }
    
    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }
    
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        client = new OrclClientImpl();
        client.initialize(uri, conf);
        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        workingDir = getHomeDirectory();
    }
    
    /**
     * Return the "current user's" home directory in this FileSystem.
     */
    public Path getHomeDirectory() {
        return this.makeQualified(new Path("/"));
    }
    
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        try {
            FileStatus fs = getFileStatus(path);
            if (fs.isFile()) {
                return new FileStatus[] { getFileStatus(path) };
            }
        } catch (FileNotFoundException e) {
        }
        
        OrclInode[] dirlist = client.listdir(path);
        
        FileStatus[] status = new FileStatus[dirlist.length];
        for (int i = 0; i < status.length; i++) {
            OrclInode inode = dirlist[i];
            Path xp = new Path(inode.getPath());
            status[i] = new FileStatus(inode.getSize(), //
                    inode.isDir(), //
                    getDefaultReplication(), //
                    getDefaultBlockSize(), //
                    inode.getAtime(), //
                    inode.getAtime(), //
                    new FsPermission(inode.getMode()), inode.getOwner(), //
                    inode.getGroup(), //
                    xp.makeQualified(getUri(), getWorkingDirectory()));
        }
        return status;
        
    }
    
    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }
    
    @Override
    public boolean mkdirs(Path path) throws IOException {
        return mkdirs(path, FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf())));
    }
    
    @Override
    public boolean mkdirs(Path path, FsPermission perms) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        client.mkdirs(path, perms.toShort());
        return true;
    }
    
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        
        OrclInode fd = client.open(path, OrclInode.O_CREAT, FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(getConf())).toShort());
        
        OrclInputStream istream = new OrclInputStream(getConf(), client, fd, fd.getSize(), bufferSize);
        return new FSDataInputStream(istream);
    }
    
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        checkPath(src);
        checkPath(dst);
        src = makeAbsolute(src);
        dst = makeAbsolute(dst);
        
        try {
            client.rename(src, dst);
        } catch (PathNotFoundException e) {
            throw new FileNotFoundException(e.getMessage());
        } catch (Exception e) {
            return false;
        }
        
        return true;
    }
    
    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        client.chmod(path, permission.toShort());
    }
    
    @Override
    public void setTimes(Path path, long mtime, long atime) throws IOException {
        checkPath(path);
        path = makeAbsolute(path);
        client.setTimes(path, atime, mtime);
    }
    
    @Override
    public void setWorkingDirectory(Path dir) {
        workingDir = makeAbsolute(dir);
    }
    
}

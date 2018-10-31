package org.apache.hadoop.fs.orcl;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;

abstract class OrclFsClient {

    abstract void chmod(Path path, short mode) throws IOException;
    
    abstract void initialize(URI uri, Configuration conf) throws IOException;
    
    abstract OrclInode[] listdir(Path path) throws IOException;
    
    abstract OrclInode lstat(Path path) throws IOException;
    
    abstract boolean mkdirs(Path path, short mode) throws IOException;
    
    abstract OrclInode open(Path path, int flags, short mode) throws IOException;
    
    abstract int read(OrclInode inode, long pos, byte[] buffer, int length) throws IOException;
    
    abstract void rename(Path src, Path dst) throws IOException;
    
    abstract void rmdir(Path path) throws IOException;
    
    abstract void setTimes(Path path, long atime, long mtime) throws IOException;
    
    abstract void close() throws IOException;
    
    abstract FsStatus statfs(Path p);
    
    abstract void unlink(Path path) throws IOException;
    
    abstract int write(OrclInode fileHandle, byte[] buffer, int bufUsed) throws IOException;
    
}

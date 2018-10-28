/*
 * 
 */
package org.apache.hadoop.fs.orcl;

import java.sql.Timestamp;

public class OrclInode {
    /** A constant indicating an FTPFile is a directory. ***/
    public static final int DIRECTORY_TYPE = 1;
    /** A constant indicating an FTPFile is a file. ***/
    public static final int FILE_TYPE = 0;
    public static final int O_APPEND = 4;
    public static final int O_CREAT = 8;
    
    public static final int O_EXCL = 32;

    public static final int O_RDONLY = 1;
    public static final int O_RDWR = 2;
    public static final int O_TRUNC = 16;
    public static final int O_WRONLY = 64;

    private Timestamp atime;
    private String group;
    private long id;
    private short mode;
    
    private Timestamp mtime;
    private String name;
    private String owner;
    private Long parent;
    private String path;
    private long size;
    private int type;
    
    public OrclInode() {
        super();
    }
    
    public OrclInode(long id, Long parent, String name, String path, int type, short mode, String owner, String group, long size, Timestamp atime, Timestamp mtime) {
        super();
        this.id = id;
        this.parent = parent;
        this.name = name;
        this.path = path;
        this.type = type;
        this.mode = mode;
        this.owner = owner;
        this.group = group;
        this.size = size;
        this.atime = atime;
        this.mtime = mtime;
    }
    
    public Timestamp getAtime() {
        return atime;
    }
    
    public String getGroup() {
        return group;
    }
    
    public long getId() {
        return id;
    }
    
    public short getMode() {
        return mode;
    }
    
    public Timestamp getMtime() {
        return mtime;
    }
    
    public String getName() {
        return name;
    }
    
    public String getOwner() {
        return owner;
    }
    
    public Long getParent() {
        return parent;
    }
    
    public String getPath() {
        return path;
    }
    
    public long getSize() {
        return size;
    }
    
    public int getType() {
        return type;
    }
    
    public boolean isDir() {
        return type == DIRECTORY_TYPE;
    }
    
    public boolean isFile() {
        return type != DIRECTORY_TYPE;
    }
    
    public void setAtime(Timestamp atime) {
        this.atime = atime;
    }
    
    public void setGroup(String group) {
        this.group = group;
    }
    
    public void setId(long id) {
        this.id = id;
    }
    
    public void setMode(short mode) {
        this.mode = mode;
    }
    
    public void setMtime(Timestamp mtime) {
        this.mtime = mtime;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public void setOwner(String owner) {
        this.owner = owner;
    }
    
    public void setParent(Long parent) {
        this.parent = parent;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setSize(long size) {
        this.size = size;
    }
    
    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "OrclInode [id=" + id + ", parent=" + parent + ", name=" + name + ", path=" + path + ", type=" + type + ", mode=" + mode + ", owner=" + owner + ", group=" + group + ", size=" + size
                + ", atime=" + atime + ", mtime=" + mtime + "]";
    }

}

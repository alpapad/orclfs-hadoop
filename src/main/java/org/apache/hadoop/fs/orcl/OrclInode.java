/*
 * 
 */
package org.apache.hadoop.fs.orcl;

public class OrclInode {
    public static final int DIRECTORY_TYPE = 1;
    public static final int FILE_TYPE = 0;

    public static final int O_APPEND = 4;
    public static final int O_CREAT = 8;
    
    public static final int O_EXCL = 32;

    public static final int O_RDONLY = 1;
    public static final int O_RDWR = 2;
    public static final int O_TRUNC = 16;
    public static final int O_WRONLY = 64;

    private final long atime;
    private final String group;
    private final long id;
    private final short mode;
    
    private final long mtime;
    private final String name;
    private final String owner;
    private final Long parent;
    private final String path;
    private final long size;
    private final int type;
    
    public OrclInode(long id, Long parent, String name, String path, int type, short mode, String owner, String group, long size, long atime, long mtime) {
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
    
    public long getAtime() {
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
    
    public long getMtime() {
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
        return getType() == DIRECTORY_TYPE;
    }
    
    public boolean isFile() {
        return getType() != DIRECTORY_TYPE;
    }

    @Override
    public String toString() {
        return "OrclInode [id=" + id + ", parent=" + getParent() + ", name=" + getName() + ", path=" + path + ", type=" + type + ", mode=" + mode + ", owner=" + owner + ", group=" + group + ", size=" + size
                + ", atime=" + getAtime() + ", mtime=" + getMtime() + "]";
    }

}

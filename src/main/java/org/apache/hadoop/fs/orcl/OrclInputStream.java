package org.apache.hadoop.fs.orcl;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;

public class OrclInputStream extends FSInputStream {
    private byte[] buffer;
    
    private int bufPos = 0;
    
    private int bufValid = 0;
    
    private OrclFsClient client;
    
    private boolean closed;
    private long fileLength;
    private OrclInode inode;
    private long streamPos = 0;
    
    public OrclInputStream(Configuration conf, OrclFsClient client, OrclInode inode, long flength, int bufferSize) {
        this.fileLength = flength;
        this.inode = inode;
        this.closed = false;
        this.client = client;
        this.buffer = new byte[512 * 1024];// - 240];
    }
    
    @Override
    public synchronized int available() throws IOException {
        if (closed) {
            throw new IOException("file is closed");
        }
        return (int) (fileLength - getPos());
    }
    
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
        }
    }
    
    private synchronized boolean fillBuffer() throws IOException {
        bufValid = client.read(inode, streamPos, buffer, buffer.length);
        bufPos = 0;
        if (bufValid < 0) {
            long err = bufValid;
            
            bufValid = 0;
            throw new IOException("Failed to fill read buffer! Error code:" + err);
        }
        streamPos += bufValid;
        return bufValid != 0;
    }
    
    @Override
    protected void finalize() throws Throwable {
        try {
            if (!closed) {
                close();
            }
        } finally {
            super.finalize();
        }
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return streamPos - bufValid + bufPos;
    }
    
    @Override
    public synchronized int read() throws IOException {

        byte result[] = new byte[1];
        
        if (getPos() >= fileLength) {
            return -1;
        }
        if (-1 == read(result, 0, 1)) {
            return -1;
        }
        if (result[0] < 0) {
            return 256 + result[0];
        } else {
            return result[0];
        }
    }
    
    @Override
    public synchronized int read(byte buf[], int off, int len) throws IOException {
        if (closed) {
            throw new IOException("OrclInputStream.read: cannot read " + len + " bytes from inode " + inode + ": stream closed");
        }
        
        // ensure we're not past the end of the file
        if (getPos() >= fileLength) {
            if (len == 0) {
                return 0;
            }
            return -1;
        }
        
        int totalRead = 0;
        int read;
        
        do {
            read = Math.min(len, bufValid - bufPos);
            System.arraycopy(buffer, bufPos, buf, off, read);
            bufPos += read;
            len -= read;
            off += read;
            totalRead += read;
        } while (len > 0 && fillBuffer());
        return totalRead;
    }
    
    @Override
    public synchronized void seek(long targetPos) throws IOException {
        if (targetPos > fileLength) {
            throw new EOFException("OrclInputStream.seek: failed seek to position " + targetPos + " on inode " + inode + ": Cannot seek after EOF " + fileLength);
        }
        if (targetPos < 0) {
            throw new EOFException("OrclInputStream.seek: failed to seek to new position " + targetPos);
        }
        streamPos = targetPos;
        bufValid = 0;
        bufPos = 0;
    }
    
    @Override
    public synchronized boolean seekToNewSource(long targetPos) {
        return false;
    }
}
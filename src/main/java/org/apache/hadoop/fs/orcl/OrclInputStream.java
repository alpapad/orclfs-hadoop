package org.apache.hadoop.fs.orcl;

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;

public class OrclInputStream extends FSInputStream {
    private static final Log LOG = LogFactory.getLog(OrclInputStream.class);
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
        this.buffer = new byte[128*1024];//  - 240];
        LOG.debug("OrclInputStream constructor: initializing stream with fh " + inode + " and file length " + flength);
        
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
        LOG.trace("OrclOutputStream.close:enter");
        if (!closed) {
            closed = true;
            LOG.trace("OrclOutputStream.close:exit");
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
        LOG.trace("OrclInputStream.read: Reading a single byte from fd " + inode + " by calling general read function");
        
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
        LOG.trace("OrclInputStream.read: Reading " + len + " bytes from fd " + inode);
        
        if (closed) {
            throw new IOException("OrclInputStream.read: cannot read " + len + " bytes from inode " + inode + ": stream closed");
        }
        
        // ensure we're not past the end of the file
        if (getPos() >= fileLength) {
            if (len == 0) {
                return 0;
            }
            LOG.debug("OrclInputStream.read: cannot read " + len + " bytes from inode " + inode + ": current position is " + getPos() + " and file length is " + fileLength);
            return -1;
        }
        
        int totalRead = 0;
        int initialLen = len;
        int read;
        
        do {
            read = Math.min(len, bufValid - bufPos);
            System.arraycopy(buffer, bufPos, buf, off, read);
//            try {
//                System.arraycopy(buffer, bufPos, buf, off, read);
//            } catch (ArrayStoreException ae) {
//                throw new IOException("OrclInputStream failed to do an array copy due to type mismatch...");
//            } catch (NullPointerException ne) {
//                throw new IOException("OrclInputStream.read: cannot read " + len + "bytes from inode:" + inode + ": buf is null");
//            }
            bufPos += read;
            len -= read;
            off += read;
            totalRead += read;
        } while (len > 0 && fillBuffer());
        
        LOG.trace("OrclInputStream.read: Reading " + initialLen + " bytes from inode " + inode + ": succeeded in reading " + totalRead + " bytes");
        return totalRead;
    }
    
    @Override
    public synchronized void seek(long targetPos) throws IOException {
        LOG.trace("OrclInputStream.seek: Seeking to position " + targetPos + " on inode " + inode);
        if (targetPos > fileLength) {
            throw new EOFException("OrclInputStream.seek: failed seek to position " + targetPos + " on inode " + inode + ": Cannot seek after EOF " + fileLength);
        }
        if (targetPos < 0) {
            throw new IOException("OrclInputStream.seek: failed to seek to new position " + targetPos);
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
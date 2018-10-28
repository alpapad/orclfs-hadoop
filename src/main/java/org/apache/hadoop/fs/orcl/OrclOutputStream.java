package org.apache.hadoop.fs.orcl;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;

public class OrclOutputStream extends OutputStream {
    private byte[] buffer;
    
    private int bufUsed = 0;
    
    private OrclFsClient client;
    
    private boolean closed;
    private OrclInode inode;
    private long streamPos = 0;
    
    public OrclOutputStream(Configuration conf, OrclFsClient client, OrclInode inode, int bufferSize) {
        this.client = client;
        this.inode = inode;
        this.closed = false;
        
        // See: https://vsadilovskiy.wordpress.com/2007/11/19/blob-write-size-and-cpu/
        this.buffer = new byte[32*1024  - 240];
    }
    
    @Override
    public synchronized void close() throws IOException {
        flush();
        closed = true;
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
    public synchronized void flush() throws IOException {
        flushBuffer();
    }
    
    private synchronized void flushBuffer() throws IOException {
        if (bufUsed == 0) {
            return;
        }
        
        while (bufUsed > 0) {
            int ret = client.write(inode, buffer, bufUsed);
            if (ret < 0) {
                throw new IOException("client.write: ret=" + ret);
            }
            
            if (ret == bufUsed) {
                bufUsed = 0;
                return;
            }
            
            assert ret > 0;
            assert ret < bufUsed;
            
            int remaining = bufUsed - ret;
            System.arraycopy(buffer, ret, buffer, 0, remaining);
            bufUsed -= ret;
        }
        
        assert bufUsed == 0;
    }
    

    public synchronized long getPos() throws IOException {
        return streamPos;
    }
    
    @Override
    public synchronized void write(byte buf[], int off, int len) throws IOException {
        while (len > 0) {
            int remaining = Math.min(len, buffer.length - bufUsed);
            System.arraycopy(buf, off, buffer, bufUsed, remaining);
            
            bufUsed += remaining;
            off += remaining;
            len -= remaining;
            
            if (buffer.length == bufUsed) {
                flushBuffer();
            }
        }
        streamPos += len;
    }
    
    @Override
    public synchronized void write(int b) throws IOException {
        byte buf[] = new byte[1];
        buf[0] = (byte) b;
        write(buf, 0, 1);
    }
}
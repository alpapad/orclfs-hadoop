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
    
    public OrclOutputStream(Configuration conf, OrclFsClient client, OrclInode inode, int bufferSize) {
        this.client = client;
        this.inode = inode;
        this.closed = false;
        this.buffer = new byte[512*1024];
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
            } else {
                throw new IOException("client.write: ret != bufUsed: " + ret + "!=" + bufUsed);
            }
        }
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
    }
    
    @Override
    public synchronized void write(int b) throws IOException {
        byte buf[] = new byte[1];
        buf[0] = (byte) b;
        write(buf, 0, 1);
    }
}
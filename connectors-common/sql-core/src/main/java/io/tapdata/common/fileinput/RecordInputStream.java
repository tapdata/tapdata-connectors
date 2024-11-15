package io.tapdata.common.fileinput;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class RecordInputStream extends InputStream {

    private final ByteBuffer buffer;

    public RecordInputStream(int capacity) {
        this.buffer = ByteBuffer.allocateDirect(capacity);
    }

    public void clear() {
        buffer.clear();
    }

    public void flip() {
        buffer.flip();
    }

    public void compact() {
        buffer.compact();
    }

    public int read() {
        if (buffer.limit() == 0) {
            return -1;
        }
        return buffer.get();
    }

    public int read(byte[] buf) {
        if (buffer.limit() == 0 || buffer.remaining() == 0) {
            return -1;
        }
        int available = buffer.remaining();
        int nRead = Math.min(available, buf.length);
        buffer.get(buf, 0, nRead);
        return nRead;
    }

    public int read(byte[] b, int off, int len) {
        if (buffer.limit() == 0) {
            return -1;
        }
        buffer.get(b, off, len);
        return b.length;
    }

    public void write(byte[] buf) throws InterruptedException {
        int wPos = 0;
        do {
            int available = buffer.remaining();
            int nWrite = Math.min(available, buf.length - wPos);
            buffer.put(buf, wPos, nWrite);
            wPos += nWrite;
        } while (wPos != buf.length);
    }
}

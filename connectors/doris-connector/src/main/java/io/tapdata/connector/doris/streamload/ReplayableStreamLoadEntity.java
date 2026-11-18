package io.tapdata.connector.doris.streamload;

import org.apache.http.entity.ByteArrayEntity;

import java.io.IOException;
import java.io.OutputStream;

/** One extra batch-sized buffer, released after execute; redirects reuse exactly the same bytes. */
final class ReplayableStreamLoadEntity extends ByteArrayEntity {
    private final StreamLoadDiagnostics diagnostics;

    ReplayableStreamLoadEntity(RecordStream stream, StreamLoadDiagnostics diagnostics) throws IOException {
        super(snapshot(stream));
        this.diagnostics = diagnostics;
        setContentEncoding("UTF-8");
        setContentType("application/json");
    }

    private static byte[] snapshot(RecordStream stream) throws IOException {
        long length = stream.getContentLength();
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new IOException("Stream load batch is too large to replay");
        }
        byte[] data = new byte[(int) length];
        byte[] buffer = new byte[8192];
        int offset = 0;
        int read;
        // Use read(byte[]): RecordStream's offset/length overload does not obey InputStream's contract.
        while ((read = stream.read(buffer)) != -1) {
            if (read <= 0 || read > data.length - offset) {
                throw new IOException("Stream load batch length mismatch");
            }
            System.arraycopy(buffer, 0, data, offset, read);
            offset += read;
        }
        if (offset != data.length) throw new IOException("Stream load batch length mismatch");
        return data;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        diagnostics.bodyWrites++;
        diagnostics.hops.add("bodyStart@" + diagnostics.elapsedMs() + "ms sinceRequestMs="
                + (diagnostics.elapsedMs() - diagnostics.requestStartedMs));
        if (diagnostics.firstBodyMs < 0) diagnostics.firstBodyMs = diagnostics.elapsedMs();
        // Count bytes accepted by the socket stream; this does not imply a committed Doris load.
        for (int offset = 0; offset < content.length; ) {
            int size = Math.min(8192, content.length - offset);
            out.write(content, offset, size);
            diagnostics.bytesWritten += size;
            offset += size;
        }
        out.flush();
    }
}

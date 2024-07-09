
package io.tapdata.pdk.cli.services.request;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.concurrent.Callable;


public class ByteArrayCollector {
    public static final String CHARSET_UTF8 = "UTF-8";
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String BOUNDARY = "00content0boundary00";
    private static final String CRLF = "\r\n";

    private static String getValidCharset(final String charset) {
        if (charset != null && charset.length() > 0)
            return charset;
        else
            return CHARSET_UTF8;
    }

    public static class HttpRequestException extends RuntimeException {

        private static final long serialVersionUID = -1170466989781746231L;

        /**
         * Create a new HttpRequestException with the given cause
         *
         * @param cause
         */
        public HttpRequestException(final IOException cause) {
            super(cause);
        }

        /**
         * Get {@link IOException} that triggered this request exception
         *
         * @return {@link IOException} cause
         */
        @Override
        public IOException getCause() {
            return (IOException) super.getCause();
        }
    }

    protected static abstract class Operation<V> implements Callable<V> {

        /**
         * Run operation
         *
         * @return result
         * @throws HttpRequestException
         * @throws IOException
         */
        protected abstract V run() throws HttpRequestException, IOException;

        /**
         * Operation complete callback
         *
         * @throws IOException
         */
        protected abstract void done() throws IOException;

        public V call() throws HttpRequestException {
            boolean thrown = false;
            try {
                return run();
            } catch (HttpRequestException e) {
                thrown = true;
                throw e;
            } catch (IOException e) {
                thrown = true;
                throw new HttpRequestException(e);
            } finally {
                try {
                    done();
                } catch (IOException e) {
                    if (!thrown)
                        throw new HttpRequestException(e);
                }
            }
        }
    }

    protected static abstract class CloseOperation<V> extends Operation<V> {

        private final Closeable closeable;

        private final boolean ignoreCloseExceptions;

        /**
         * Create closer for operation
         *
         * @param closeable
         * @param ignoreCloseExceptions
         */
        protected CloseOperation(final Closeable closeable,
                                 final boolean ignoreCloseExceptions) {
            this.closeable = closeable;
            this.ignoreCloseExceptions = ignoreCloseExceptions;
        }

        @Override
        protected void done() throws IOException {
            if (closeable instanceof Flushable)
                ((Flushable) closeable).flush();
            if (ignoreCloseExceptions)
                try {
                    closeable.close();
                } catch (IOException e) {
                    // Ignored
                }
            else
                closeable.close();
        }
    }

    public static class RequestOutputStream extends ByteArrayOutputStream {
        private final CharsetEncoder encoder;

        public RequestOutputStream(final String charset, final int bufferSize) {
            super(bufferSize);
            encoder = Charset.forName(getValidCharset(charset)).newEncoder();
        }

        public RequestOutputStream write(final String value) throws IOException {
            final ByteBuffer bytes = encoder.encode(CharBuffer.wrap(value));
            super.write(bytes.array(), 0, bytes.limit());
            return this;
        }
    }

    public RequestOutputStream getOutput() {
        return output;
    }

    private RequestOutputStream output;

    private boolean multipart;

    private final boolean ignoreCloseExceptions = true;


    private final int bufferSize = 8192;

    public ByteArrayCollector() {
    }

    protected ByteArrayCollector copy(final InputStream input, final OutputStream output) {
        return new CloseOperation<ByteArrayCollector>(input, ignoreCloseExceptions) {
            @Override
            public ByteArrayCollector run() throws IOException {
                final byte[] buffer = new byte[bufferSize];
                int read;
                while ((read = input.read(buffer)) != -1) {
                    output.write(buffer, 0, read);
                }
                return ByteArrayCollector.this;
            }
        }.call();
    }

    protected ByteArrayCollector openOutput() {
        if (output != null)
            return this;
        output = new RequestOutputStream("utf-8", bufferSize);
        return this;
    }

    /**
     * Start part of a multipart
     *
     * @return this request
     * @throws IOException
     */
    protected ByteArrayCollector startPart() throws IOException {
        if (!multipart) {
            multipart = true;
            openOutput();
            output.write("--" + BOUNDARY + CRLF);
        } else
            output.write(CRLF + "--" + BOUNDARY + CRLF);
        return this;
    }

    protected ByteArrayCollector writePartHeader(final String name,
                                                 final String filename, final String contentType) throws IOException {
        final StringBuilder partBuffer = new StringBuilder();
        partBuffer.append("form-data; name=\"").append(name);
        if (filename != null)
            partBuffer.append("\"; filename=\"").append(filename);
        partBuffer.append('"');
        partHeader("Content-Disposition", partBuffer.toString());
        if (contentType != null)
            partHeader(HEADER_CONTENT_TYPE, contentType);
        return send(CRLF);
    }

    public ByteArrayCollector part(final String name, final String part) {
        return part(name, null, part);
    }


    public ByteArrayCollector part(final String name, final String filename,
                                   final String part) throws HttpRequestException {
        return part(name, filename, null, part);
    }

    public ByteArrayCollector part(final String name, final String filename,
                                   final String contentType, final String part) throws HttpRequestException {
        try {
            startPart();
            writePartHeader(name, filename, contentType);
            output.write(part);
        } catch (IOException e) {
            throw new HttpRequestException(e);
        }
        return this;
    }

    public ByteArrayCollector part(final String name, final String filename,
                                   final String contentType, final File part) throws HttpRequestException {
        final InputStream stream;
        try {
            stream = new BufferedInputStream(new FileInputStream(part));
        } catch (IOException e) {
            throw new HttpRequestException(e);
        }
        return part(name, filename, contentType, stream);
    }


    public ByteArrayCollector part(final String name, final String filename,
                                   final String contentType, final InputStream part)
            throws HttpRequestException {
        try {
            startPart();
            writePartHeader(name, filename, contentType);
            copy(part, output);
        } catch (IOException e) {
            throw new HttpRequestException(e);
        }
        return this;
    }

    public ByteArrayCollector partHeader(final String name, final String value)
            throws HttpRequestException {
        return send(name).send(": ").send(value).send(CRLF);
    }

    public ByteArrayCollector send(final CharSequence value) throws HttpRequestException {
        try {
            openOutput();
            output.write(value.toString());
        } catch (IOException e) {
            throw new HttpRequestException(e);
        }
        return this;
    }
}

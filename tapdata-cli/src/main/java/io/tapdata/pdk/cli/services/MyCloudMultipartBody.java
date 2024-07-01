package io.tapdata.pdk.cli.services;

import io.tapdata.pdk.cli.services.request.ProgressRequestBody;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import okhttp3.internal.Util;
import okio.Buffer;
import okio.BufferedSink;
import okio.ByteString;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class MyCloudMultipartBody extends RequestBody {

    public static final MediaType MIXED = MediaType.parse("multipart/mixed");


    public static final MediaType ALTERNATIVE = MediaType.parse("multipart/alternative");


    public static final MediaType DIGEST = MediaType.parse("multipart/digest");


    public static final MediaType PARALLEL = MediaType.parse("multipart/parallel");


    public static final MediaType FORM = MediaType.parse("multipart/form-data");

    private static final byte[] COLONSPACE = {':', ' '};
    private static final byte[] CRLF = {'\r', '\n'};
    private static final byte[] DASHDASH = {'-', '-'};

    private final ByteString boundary;
    private final MediaType originalType;
    private final MediaType contentType;
    private final List<Part> parts;
    private long contentLength = -1L;

    MyCloudMultipartBody(ByteString boundary, MediaType type, List<Part> parts) {
        this.boundary = boundary;
        this.originalType = type;
        this.contentType = MediaType.parse(type + "; boundary=" + boundary.utf8());
        this.parts = Util.immutableList(parts);
    }

    public MediaType type() {
        return originalType;
    }

    public String boundary() {
        return boundary.utf8();
    }


    public int size() {
        return parts.size();
    }

    public List<Part> parts() {
        return parts;
    }

    public Part part(int index) {
        return parts.get(index);
    }


    @Override
    public MediaType contentType() {
        return contentType;
    }

    @Override
    public long contentLength() throws IOException {
        long result = contentLength;
        if (result != -1L) return result;
        return contentLength = countBytes();
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        writeOrCountBytes(sink);
    }

    private long countBytes() throws IOException {
        long byteCount = 0L;

        Buffer byteCountBuffer = new Buffer();

        for (int p = 0, partCount = parts.size(); p < partCount; p++) {
            Part part = parts.get(p);
            Headers headers = part.headers;
            RequestBody body = part.body;

            byteCountBuffer.write(DASHDASH);
            byteCountBuffer.write(boundary);
            byteCountBuffer.write(CRLF);

            if (headers != null) {
                for (int h = 0, headerCount = headers.size(); h < headerCount; h++) {
                    byteCountBuffer.writeUtf8(headers.name(h))
                            .write(COLONSPACE)
                            .writeUtf8(headers.value(h))
                            .write(CRLF);
                }
            }

            MediaType contentType = body.contentType();
            if (contentType != null) {
                byteCountBuffer.writeUtf8("Content-Type: ")
                        .writeUtf8(contentType.toString())
                        .write(CRLF);
            }

            long contentLength = body.contentLength();
            if (contentLength != -1) {
                byteCountBuffer.writeUtf8("Content-Length: ")
                        .writeDecimalLong(contentLength)
                        .write(CRLF);
            } else {
                // We can't measure the body's size without the sizes of its components.
                byteCountBuffer.clear();
                return -1L;
            }

            byteCountBuffer.write(CRLF);
            byteCount += contentLength;
            byteCountBuffer.write(CRLF);
        }

        byteCountBuffer.write(DASHDASH);
        byteCountBuffer.write(boundary);
        byteCountBuffer.write(DASHDASH);
        byteCountBuffer.write(CRLF);
        byteCount += byteCountBuffer.size();
        byteCountBuffer.clear();
        return byteCount;
    }


    private void writeOrCountBytes(@Nullable BufferedSink sink) throws IOException {
        for (int p = 0, partCount = parts.size(); p < partCount; p++) {
            Part part = parts.get(p);
            Headers headers = part.headers;
            ProgressRequestBody<?> body = (ProgressRequestBody<?>) part.body;

            sink.write(DASHDASH);
            sink.write(boundary);
            sink.write(CRLF);
            if (headers != null) {
                for (int h = 0, headerCount = headers.size(); h < headerCount; h++) {
                    sink.writeUtf8(headers.name(h))
                            .write(COLONSPACE)
                            .writeUtf8(headers.value(h))
                            .write(CRLF);
                }
            }

            MediaType contentType = body.contentType();
            if (contentType != null) {
                sink.writeUtf8("Content-Type: ")
                        .writeUtf8(contentType.toString())
                        .write(CRLF);
            }

            long contentLength = body.contentLength();
            if (contentLength != -1L) {
                sink.writeUtf8("Content-Length: ")
                        .writeDecimalLong(contentLength)
                        .write(CRLF);
            }

            sink.write(CRLF);
            body.writeTo(sink);
            sink.write(CRLF);
        }
        sink.write(DASHDASH);
        sink.write(boundary);
        sink.write(DASHDASH);
        sink.write(CRLF);
    }

    /**
     * Appends a quoted-string to a StringBuilder.
     *
     * <p>RFC 2388 is rather vague about how one should escape special characters in form-data
     * parameters, and as it turns out Firefox and Chrome actually do rather different things, and
     * both say in their comments that they're not really sure what the right approach is. We go with
     * Chrome's behavior (which also experimentally seems to match what IE does), but if you actually
     * want to have a good chance of things working, please avoid double-quotes, newlines, percent
     * signs, and the like in your field names.
     */
    static StringBuilder appendQuotedString(StringBuilder target, String key) {
        target.append('"');
        for (int i = 0, len = key.length(); i < len; i++) {
            char ch = key.charAt(i);
            switch (ch) {
                case '\n':
                    target.append("%0A");
                    break;
                case '\r':
                    target.append("%0D");
                    break;
                case '"':
                    target.append("%22");
                    break;
                default:
                    target.append(ch);
                    break;
            }
        }
        target.append('"');
        return target;
    }

    public static final class Part {
        public static Part create(RequestBody body) {
            return create(null, body);
        }

        public static Part create(@Nullable Headers headers, RequestBody body) {
            if (body == null) {
                throw new NullPointerException("body == null");
            }
            if (headers != null && headers.get("Content-Type") != null) {
                throw new IllegalArgumentException("Unexpected header: Content-Type");
            }
            if (headers != null && headers.get("Content-Length") != null) {
                throw new IllegalArgumentException("Unexpected header: Content-Length");
            }
            return new Part(headers, body);
        }

        public static Part createFormData(String name, String value) {
            return createFormData(name, null, RequestBody.create(null, value));
        }

        public static Part createFormData(String name, @Nullable String filename, RequestBody body) {
            if (name == null) {
                throw new NullPointerException("name == null");
            }
            StringBuilder disposition = new StringBuilder("form-data; name=");
            appendQuotedString(disposition, name);

            if (filename != null) {
                disposition.append("; filename=");
                appendQuotedString(disposition, filename);
            }

            return create(Headers.of("Content-Disposition", disposition.toString()), body);
        }

        final @Nullable
        Headers headers;
        final RequestBody body;

        private Part(@Nullable Headers headers, RequestBody body) {
            this.headers = headers;
            this.body = body;
        }

        public @Nullable
        Headers headers() {
            return headers;
        }

        public RequestBody body() {
            return body;
        }
    }

    public static final class Builder {
        private final ByteString boundary;
        private MediaType type = MIXED;
        private final List<Part> parts = new ArrayList<>();

        public Builder() {
            this(UUID.randomUUID().toString());
        }

        public Builder(String boundary) {
            this.boundary = ByteString.encodeUtf8(boundary);
        }

        /**
         * Set the MIME type. Expected values for {@code type} are {@link #MIXED} (the default), {@link
         * #ALTERNATIVE}, {@link #DIGEST}, {@link #PARALLEL} and {@link #FORM}.
         */
        public Builder setType(MediaType type) {
            if (type == null) {
                throw new NullPointerException("type == null");
            }
            if (!type.type().equals("multipart")) {
                throw new IllegalArgumentException("multipart != " + type);
            }
            this.type = type;
            return this;
        }

        /**
         * Add a part to the body.
         */
        public Builder addPart(RequestBody body) {
            return addPart(Part.create(body));
        }

        /**
         * Add a part to the body.
         */
        public Builder addPart(@Nullable Headers headers, RequestBody body) {
            return addPart(Part.create(headers, body));
        }

        /**
         * Add a form data part to the body.
         */
        public Builder addFormDataPart(String name, String value) {
            return addPart(Part.createFormData(name, value));
        }

        /**
         * Add a form data part to the body.
         */
        public Builder addFormDataPart(String name, @Nullable String filename, RequestBody body) {
            return addPart(Part.createFormData(name, filename, body));
        }

        /**
         * Add a part to the body.
         */
        public Builder addPart(Part part) {
            if (part == null) throw new NullPointerException("part == null");
            parts.add(part);
            return this;
        }

        /**
         * Assemble the specified parts into a request body.
         */
        public MyCloudMultipartBody build() {
            if (parts.isEmpty()) {
                throw new IllegalStateException("Multipart body must have at least one part.");
            }
            return new MyCloudMultipartBody(boundary, type, parts);
        }
    }
}

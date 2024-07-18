package io.tapdata.connector.mysql.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author samuel
 * @Description
 * @create 2022-05-25 17:41
 **/
public class StringCompressUtil {
    private static final String DEFAULT_CODE = "ISO-8859-1";

    public static String compress(String str) throws IOException {
        if (str == null || str.length() == 0) {
            return str;
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(str.getBytes());
            String string = out.toString(DEFAULT_CODE);
            return string;
        }
    }

    public static String uncompress(String str) throws IOException {
        if (str == null || str.length() == 0) {
            return str;
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes(DEFAULT_CODE));
             GZIPInputStream gunzip = new GZIPInputStream(in)) {
            byte[] buffer = new byte[256];
            int n;
            while ((n = gunzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            String string = out.toString(DEFAULT_CODE);
            return string;
        }
    }
}
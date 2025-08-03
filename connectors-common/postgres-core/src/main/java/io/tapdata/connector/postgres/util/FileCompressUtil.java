package io.tapdata.connector.postgres.util;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;

public class FileCompressUtil {

    public static void extractTarGz(InputStream sourcePath, String destDir) throws IOException {
        try (
                GzipCompressorInputStream gis = new GzipCompressorInputStream(sourcePath);
                TarArchiveInputStream tis = new TarArchiveInputStream(gis)
        ) {
            ArchiveEntry entry;
            while ((entry = tis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    new File(destDir, entry.getName()).mkdirs();
                } else {
                    File outputFile = new File(destDir, entry.getName());
                    if (!outputFile.getParentFile().exists()) {
                        outputFile.getParentFile().mkdirs(); // 创建必要的父目录
                    }
                    outputFile.createNewFile(); // 创建文件
                    FileOutputStream fos = new FileOutputStream(outputFile);
                    BufferedOutputStream bos = new BufferedOutputStream(fos);
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = tis.read(buffer)) != -1) {
                        bos.write(buffer, 0, len);
                    }
                    bos.close();
                    fos.close();
                }
            }
        }
    }
}

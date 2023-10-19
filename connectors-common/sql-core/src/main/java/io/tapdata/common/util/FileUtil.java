package io.tapdata.common.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileUtil {

    public static void save(byte[] data, String savePath, boolean deleteExists) throws IOException {
        File file = new File(savePath);
        if (file.exists()) {
            if (!deleteExists) {
                throw new RuntimeException("Save file is exists: " + savePath);
            }
            file.delete();
        } else {
            File dir = file.getParentFile();
            if (!dir.exists()) {
                dir.mkdirs();
            }
        }
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(data);
        }
    }

    public static String paths(String... paths) {
        return String.join(File.separator, paths);
    }

    public static String storeDir(String typeDir) {
        String dir = System.getenv("TAPDATA_WORK_DIR");
        if (null == dir) {
            dir = ".";
        }
        return paths(dir, typeDir);
    }

}

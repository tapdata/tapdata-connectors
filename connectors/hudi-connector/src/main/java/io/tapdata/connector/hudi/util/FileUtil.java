package io.tapdata.connector.hudi.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Base64;

public class FileUtil {

    public static void saveBase64File(String baseDir, String fileName, String base64Value, boolean deleteExists) throws IOException {
        String savePath = FileUtil.paths(baseDir, fileName);
        byte[] bytes = Base64.getDecoder().decode(base64Value);
        save(bytes, savePath, deleteExists);
    }

    public static String storeDir(String pathName) {
        String dir = System.getenv("TAPDATA_WORK_DIR");
        if (null == dir) {
            dir = System.getProperty("user.dir");
        }
        return paths(dir, pathName);
    }

    public static String paths(String... paths) {
        return String.join(File.separator, paths);
    }

    /**
     * 保存文件
     * @auth kiki
     * @param data         数据
     * @param savePath     保存路径
     * @param deleteExists 是否存在删除
     * @throws IOException 异常
     */
    public static void save(byte[] data, String savePath, boolean deleteExists) throws IOException {
        File file = new File(savePath);
        if (file.exists()) {
            if (!deleteExists) {
                throw new RuntimeException("Save config is exists: " + savePath);
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
}

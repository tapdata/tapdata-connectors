package io.tapdata.connector.hudi.util;

import io.tapdata.entity.error.CoreException;

public class SiteXMLUtil {
    public static final String BASE_PATH_TAG = "xml_conf";
    public static final String CORE_SITE_NAME = "core-site.xml";
    public static final String HDFS_SITE_NAME = "hdfs-site.xml";
    public static final String HIVE_SITE_NAME = "hive-site.xml";


    public static String saveSiteXMLS(String catalog, String coreSiteBase64Value, String hdfsXMLBase64Value, String hiveSiteBase64Value) {
        String dir = FileUtil.paths(FileUtil.storeDir(BASE_PATH_TAG), catalog);
        final String failMessageRegex = "Fail to save conf file to path: {}, file name: {}";
        saveFile(dir, CORE_SITE_NAME, coreSiteBase64Value,  true, failMessageRegex);
        saveFile(dir, HDFS_SITE_NAME, hdfsXMLBase64Value,  true, failMessageRegex);
        saveFile(dir, HIVE_SITE_NAME, hiveSiteBase64Value,  true, failMessageRegex);
        return dir;
    }

    private static void saveFile(String basePath, String saveName, String base64Value, boolean deleteExists, String errorMessageRegex) {
        try {
            FileUtil.saveBase64File(basePath, saveName, base64Value, deleteExists);
        } catch (Exception e) {
            throw new CoreException(errorMessageRegex, basePath, saveName, e);
        }
    }
}

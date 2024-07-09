package io.tapdata.pdk.cli.services;

import io.tapdata.pdk.cli.services.server.DoCloudUpload;
import io.tapdata.pdk.cli.services.server.DoOpUpload;
import io.tapdata.pdk.cli.utils.PrintUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UploadServiceWithProcess implements Uploader {
    boolean cloud;
    ServiceUpload uploader;

    public UploadServiceWithProcess(PrintUtil printUtil, String hp, String ak, String sk, String accessCode, boolean latest) {
        cloud = StringUtils.isNotBlank(ak);
        if (!cloud) {
            uploader = new DoOpUpload(printUtil, hp, accessCode, latest);
        } else {
            uploader = new DoCloudUpload(printUtil, hp, ak, sk, latest);
        }
    }

    public void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons, String connectionType) {
        assert file != null;
        Map<String, BufferedInputStream> map = new HashMap<>();
        if (null != inputStreamMap && !inputStreamMap.isEmpty()) {
            inputStreamMap.forEach((name, stream) -> {
                map.put(name, new BufferedInputStream(stream, 1024 * 1024 * 10));
            });
        }
        uploader.upload(map, file, jsons, connectionType);
    }
}

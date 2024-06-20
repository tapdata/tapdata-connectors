package io.tapdata.pdk.cli.services;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface Uploader {
    public void upload(Map<String, InputStream> inputStreamMap, File file, List<String> jsons);
}

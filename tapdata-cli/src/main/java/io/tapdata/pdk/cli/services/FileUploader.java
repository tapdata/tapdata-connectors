package io.tapdata.pdk.cli.services;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class FileUploader {
    public static void main(String[] args) {
        String[] filePaths = {"path/to/your/file1", "path/to/your/file2"};
        String uploadUrl = "http://example.com/upload";

        try {
            uploadFilesWithProgress(filePaths, uploadUrl);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void uploadFilesWithProgress(String[] filePaths, String uploadUrl) throws IOException {
        String boundary = "===" + System.currentTimeMillis() + "===";
        String LINE_FEED = "\r\n";

        URL url = new URL(uploadUrl);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setUseCaches(false);
        httpConn.setDoOutput(true);
        httpConn.setDoInput(true);
        httpConn.setRequestMethod("POST");
        httpConn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

        try (DataOutputStream request = new DataOutputStream(httpConn.getOutputStream())) {
            long totalBytes = 0;
            for (String filePath : filePaths) {
                totalBytes += new File(filePath).length();
            }
            long bytesTransferred = 0;

            for (String filePath : filePaths) {
                File file = new File(filePath);
                String fileName = file.getName();

                request.writeBytes("--" + boundary + LINE_FEED);
                request.writeBytes("Content-Disposition: form-data; name=\"files\"; filename=\"" + fileName + "\"" + LINE_FEED);
                request.writeBytes("Content-Type: " + "application/octet-stream" + LINE_FEED);
                request.writeBytes(LINE_FEED);

                try (FileInputStream inputStream = new FileInputStream(file)) {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        request.write(buffer, 0, bytesRead);
                        bytesTransferred += bytesRead;
                        int progress = (int) ((bytesTransferred * 100) / totalBytes);
                        System.out.println("Upload progress: " + progress + "%");
                    }
                }

                request.writeBytes(LINE_FEED);
            }

            request.writeBytes("--" + boundary + "--" + LINE_FEED);
            request.flush();
        }

        int responseCode = httpConn.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            System.out.println("Files uploaded successfully.");
        } else {
            System.out.println("Failed to upload files. Response code: " + responseCode);
        }
    }
}


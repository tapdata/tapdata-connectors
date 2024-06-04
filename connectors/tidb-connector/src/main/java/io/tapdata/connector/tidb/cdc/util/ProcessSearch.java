package io.tapdata.connector.tidb.cdc.util;

import io.tapdata.entity.logger.Log;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class ProcessSearch {
    private ProcessSearch() {
    }

    public static void main(String[] args) {
        // 关键字列表，可以根据需要修改
        List<String> keywords = new ArrayList<>();
        keywords.add("/Applications/Google");
        keywords.add("Chrome.app/Contents/Frameworks/Google");
        keywords.add("Chrome");
        //keywords.add("java");
        keywords.add("Framework.framework/Versions/125.0.6422.113/Helpers/Google");
        List<String> processes = getProcesses(null, (String[]) keywords.toArray());
        processes.forEach(System.out::println);
    }

    public static String[] getCommand(String command) {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            return new String[]{"cmd.exe", "/c", command};
        } else {
            return new String[]{"/bin/sh", "-c", command};
        }
    }

    protected static String getSearchCommand(Log log, String... keywords) {
        String os = System.getProperty("os.name").toLowerCase();
        String command = "";
        if (os.contains("win")) {
            return  "tasklist";
        } else if (os.contains("nix") || os.contains("nux") || os.contains("mac")) {
            return  "ps -e" + grep(keywords);
        } else {
            log.warn("Unsupported operating system: {}", os);
            return null;
        }
    }

    protected static String grep(String... keywords) {
        if (null == keywords || keywords.length == 0) return "";
        StringJoiner joiner = new StringJoiner("/ && /");
        for (String keyword : keywords) {
            joiner.add(keyword);
        }
        return " | awk /" + joiner.toString() + "/";
    }

    public static List<String> getProcesses(Log log, String... keywords) {
        List<String> processes = new ArrayList<>();
        BufferedReader reader = null;
        ProcessBuilder processBuilder = new ProcessBuilder();
        String searchCommand = getSearchCommand(log, keywords);
        if (null == searchCommand) return processes;
        processBuilder.command(getCommand(searchCommand));
        try {
            Process process = processBuilder.start();
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                processes.add(line);
            }
        } catch (Exception e) {
            log.warn("Process search failed: {}", e.getMessage());
        } finally {
            if (null != reader) {
                try {
                    reader.close();
                } catch (Exception e) {
                    log.warn("Process reader failed: {}", e.getMessage());
                }
            }
        }
        return processes;
    }
}

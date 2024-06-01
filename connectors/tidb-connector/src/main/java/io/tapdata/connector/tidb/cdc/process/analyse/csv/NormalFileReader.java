package io.tapdata.connector.tidb.cdc.process.analyse.csv;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public class NormalFileReader {

    public List<String> read(String csvPath, boolean negate) {
        List<String> lines = new ArrayList<>();
        try (
                FileInputStream inputStream = new FileInputStream(csvPath);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            String line = null;
            while (null != (line = bufferedReader.readLine())) {
                if ("".equals(line.trim())) continue;
                lines.add(line.length() > 300 ? (line.substring(0, 300) + "...") : line);
            }
        } catch (Exception e) {
            throw new CoreException("Monitor can not handle csv line, msg: " + e.getMessage());
        }
        if (negate && lines.size() > 1) {
            Collections.reverse(lines);
        }
        return lines;
    }

    public void readLineByLine(File file, ReadConsumer<String> consumer) {
        try (
                FileInputStream inputStream = new FileInputStream(file);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            String line = null;
            while (null != (line = bufferedReader.readLine())) {
                if ("".equals(line.trim())) continue;
                consumer.accept(line);
            }
        } catch (Exception e) {
            throw new CoreException("Monitor can not handle cdc data line, msg: " + e.getMessage());
        }
    }

    public String readAll(File file) {
        StringBuilder builder = new StringBuilder();
        try (
                FileInputStream inputStream = new FileInputStream(file);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            String line = null;
            while (null != (line = bufferedReader.readLine())) {
                builder.append(line);
            }
        } catch (Exception e) {
            throw new CoreException("Monitor can not handle cdc data line, msg: " + e.getMessage());
        }
        return builder.toString();
    }


    public static final int MAX_READ_LINE = 50;
    public String readString(String csvPath, boolean negate) {
        List<String> read = read(csvPath, negate);
        StringJoiner builder = new StringJoiner("\n");
        int lines = 0;
        if (null != read && !read.isEmpty()) {
            for (String s : read) {
                builder.add(s);
                lines++;
                if (lines >= MAX_READ_LINE) break;
            }
        } else {
            builder.add("-");
        }
        return builder.toString();
    }

    private Log log;

    public void setLog(Log log) {
        this.log = log;
    }
}

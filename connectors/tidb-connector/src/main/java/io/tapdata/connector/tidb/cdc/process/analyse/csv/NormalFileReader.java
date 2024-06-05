package io.tapdata.connector.tidb.cdc.process.analyse.csv;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

public class NormalFileReader {
    public void readLineByLine(File file, ReadConsumer<String> consumer, Supplier<Boolean> alive) {
        try (
                FileInputStream inputStream = new FileInputStream(file);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            String line = null;
            while (null != (line = bufferedReader.readLine()) && alive.get()) {
                if ("".equals(line.trim())) continue;
                consumer.accept(line);
            }
        } catch (Exception e) {
            throw new CoreException("Monitor can not handle cdc data line, msg: " + e.getMessage());
        }
    }

    public String readAll(File file, Supplier<Boolean> alive) {
        StringBuilder builder = new StringBuilder();
        try (
                FileInputStream inputStream = new FileInputStream(file);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            String line = null;
            while (null != (line = bufferedReader.readLine()) && alive.get()) {
                builder.append(line);
            }
        } catch (Exception e) {
            throw new CoreException("Monitor can not handle cdc data line, msg: " + e.getMessage());
        }
        return builder.toString();
    }

    private Log log;

    public void setLog(Log log) {
        this.log = log;
    }
}

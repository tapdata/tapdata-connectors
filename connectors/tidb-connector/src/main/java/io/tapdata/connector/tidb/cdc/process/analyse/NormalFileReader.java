package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.entity.error.CoreException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;

public class NormalFileReader {

    public void readLineByLine(File file, ReadConsumer<String> consumer, BooleanSupplier alive) {
        try (
                FileInputStream inputStream = new FileInputStream(file);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader)
        ) {
            String line = null;
            while (null != (line = bufferedReader.readLine()) && alive.getAsBoolean()) {
                if ("".equals(line.trim())) continue;
                consumer.accept(line);
            }
        } catch (Exception e) {
            throw new CoreException(0, e, "Monitor can not handle cdc data line, msg: {}", e.getMessage());
        }
    }

    public String readAll(File file, BooleanSupplier alive) {
        StringBuilder builder = new StringBuilder();
        readLineByLine(file, builder::append, alive);
        return builder.toString();
    }
}

package io.tapdata.connector.tidb.cdc.util;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ProcessLauncher {
    private ProcessLauncher() {
    }

    public static String execCmdWaitResult(String cmd, String errorMsg, Log log) {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = null;
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(System.lineSeparator());
                sb.append(line);
            }
            br.close();
            br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            while ((line = br.readLine()) != null) {
                sb.append(System.lineSeparator());
                sb.append(line);
            }
        } catch (Exception e) {
            log.warn(errorMsg, e.getMessage());
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                }
            }
        }
        log.debug(sb.toString());
        return sb.toString();
    }

    protected static Process doRun(Process process) {
        try {
            process.exitValue();
        } catch (Exception e) {
            return process;
        }
        throw new CoreException("Cdc tool can not running, fail to get stream data");
    }

    public static Process startProcessInBackground(String[] command) {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.inheritIO();
        try {
            return doRun(processBuilder.start());
        } catch (IOException e) {
            throw new CoreException("Cdc tool can not running, fail start process: {}", e.getMessage(), e);
        }
    }
}

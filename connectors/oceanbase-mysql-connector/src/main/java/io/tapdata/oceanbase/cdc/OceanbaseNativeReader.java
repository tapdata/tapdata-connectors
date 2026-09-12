package io.tapdata.oceanbase.cdc;

import io.netty.util.BooleanSupplier;
import io.tapdata.data.ob.ReadLogPayload;
import io.tapdata.entity.logger.Log;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import io.tapdata.oceanbase.runtime.OceanbaseCdcRuntime;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Native CDC reader: spawns one obcdc-helper subprocess per task which drives
 * libobcdc directly and streams "4-byte big-endian length + ReadLogPayload"
 * frames on stdout. Replaces the ob-log-decoder gRPC server and loads the
 * packaged obcdc runtime from Tapdata's shared native cache.
 */
public class OceanbaseNativeReader extends OceanbaseReaderV2 {

    private static final String HELPER_NAME = "obcdc-helper";
    private static final String HELPER_RESOURCE = "/native/obcdc/4.4.2.1/linux-x86_64/obcdc-helper";

    private final Log tapLogger;
    private volatile Process process;
    private File workDir;

    public OceanbaseNativeReader(OceanbaseConfig oceanbaseConfig, String connectorId, Log tapLogger) {
        super(oceanbaseConfig, connectorId);
        this.tapLogger = tapLogger;
    }

    @Override
    protected void initSource() {
        // no gRPC stub in native mode
    }

    @Override
    public void start(BooleanSupplier isAlive) throws Throwable {
        File helper = extractHelper();
        ProcessBuilder pb = new ProcessBuilder(helper.getAbsolutePath()).directory(workDir);
        process = pb.start();
        tapLogger.info("obcdc-helper started, pid dir: {}", workDir.getAbsolutePath());

        Thread stderrPump = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    tapLogger.info("[obcdc-helper] {}", line);
                }
            } catch (IOException ignore) {
            }
        });
        stderrPump.setName("OceanBaseNativeReader-StderrPump");
        stderrPump.setDaemon(true);
        stderrPump.start();

        // configuration goes through stdin so credentials never appear on argv
        try (Writer writer = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8)) {
            writeConf(writer, "_task_id", connectorId);
            writeConf(writer, "_start_timestamp", String.valueOf((Long) offsetState));
            writeConf(writer, "_conf_template", new File(workDir, "obcdc/etc/libobcdc.conf").getAbsolutePath());
            writeConf(writer, "cluster_user", oceanbaseConfig.getCdcUser());
            writeConf(writer, "cluster_password", oceanbaseConfig.getCdcPassword());
            writeConf(writer, "rootserver_list", oceanbaseConfig.getRootServerList());
            writeConf(writer, "tb_white_list", tableList.stream()
                    .map(table -> oceanbaseConfig.getTenant() + "." + oceanbaseConfig.getDatabase() + "." + table)
                    .collect(Collectors.joining("|")));
        } catch (IOException e) {
            // stdin pipe broken => the helper died before accepting configuration
            throw helperEarlyExit(e);
        }

        DataInputStream dataIn = new DataInputStream(process.getInputStream());
        try {
            run(new FrameIterator(dataIn), isAlive);
            checkUnexpectedExit(isAlive);
        } finally {
            stop();
        }
    }

    /** Build a descriptive error for a helper that exited before reading its config. */
    private Throwable helperEarlyExit(IOException cause) {
        Integer exitCode = null;
        try {
            if (process.waitFor(3, TimeUnit.SECONDS)) {
                exitCode = process.exitValue();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return new IllegalStateException("obcdc-helper exited immediately"
                + (exitCode == null ? "" : " (exit code " + exitCode + ")")
                + " before accepting configuration; see preceding [obcdc-helper] log lines."
                + " The packaged OceanBase CDC runtime may be missing, corrupted, or unsupported on this host", cause);
    }

    /** Raise an error when the helper closed its stdout while the task is still running. */
    private void checkUnexpectedExit(BooleanSupplier isAlive) throws Throwable {
        if (!isAlive.get()) {
            return; // task is stopping, helper exit is expected
        }
        Process p = process;
        Integer exitCode = null;
        if (p != null && p.waitFor(3, TimeUnit.SECONDS)) {
            exitCode = p.exitValue();
        }
        throw new IllegalStateException("obcdc-helper exited unexpectedly"
                + (exitCode == null ? "" : " (exit code " + exitCode + ")")
                + " while the task is still running; see preceding [obcdc-helper] log lines for the obcdc error");
    }

    @Override
    public void stop() {
        Process p = process;
        if (p == null) {
            return;
        }
        process = null;
        p.destroy(); // SIGTERM, helper stops obcdc and exits
        try {
            if (!p.waitFor(30, TimeUnit.SECONDS)) {
                tapLogger.warn("obcdc-helper did not exit within 30s, killing");
                p.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            p.destroyForcibly();
        }
    }

    private static void writeConf(Writer writer, String key, String value) throws IOException {
        writer.write(key + "=" + (value == null ? "" : value) + "\n");
    }

    private File extractHelper() throws IOException {
        workDir = Files.createTempDirectory("obcdc-" + connectorId).toFile();
        File runtimeHome = OceanbaseCdcRuntime.prepare(OceanbaseNativeReader.class);
        Files.createSymbolicLink(new File(workDir, "obcdc").toPath(), runtimeHome.toPath());
        ClassLoader classLoader = OceanbaseNativeReader.class.getClassLoader();
        if (classLoader == null) {
            classLoader = ClassLoader.getSystemClassLoader();
        }
        InputStream in = classLoader.getResourceAsStream(HELPER_RESOURCE.substring(1));
        if (in == null) {
            in = OceanbaseNativeReader.class.getResourceAsStream(HELPER_RESOURCE);
        }
        if (in == null) {
            throw new IOException(HELPER_RESOURCE + " not found in connector package");
        }
        File helper = new File(workDir, HELPER_NAME);
        try (OutputStream out = new FileOutputStream(helper)) {
            byte[] buffer = new byte[8192];
            int n;
            while ((n = in.read(buffer)) != -1) {
                out.write(buffer, 0, n);
            }
        } finally {
            in.close();
        }
        if (!helper.setExecutable(true, true)) {
            throw new IOException("cannot make " + helper.getAbsolutePath() + " executable");
        }
        return helper;
    }

    /**
     * Blocking iterator over length-prefixed ReadLogPayload frames; ends when
     * the helper closes its stdout (process exit).
     */
    private static class FrameIterator implements Iterator<ReadLogPayload> {
        private final DataInputStream in;
        private ReadLogPayload next;
        private boolean eof;

        FrameIterator(DataInputStream in) {
            this.in = in;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            if (eof) {
                return false;
            }
            try {
                int len = in.readInt();
                if (len < 0 || len > 256 * 1024 * 1024) {
                    throw new IOException("invalid frame length " + len + ", stream corrupted");
                }
                byte[] body = new byte[len];
                in.readFully(body);
                next = ReadLogPayload.parseFrom(body);
                return true;
            } catch (EOFException e) {
                eof = true;
                return false;
            } catch (IOException e) {
                eof = true;
                throw new RuntimeException("failed to read frame from obcdc-helper", e);
            }
        }

        @Override
        public ReadLogPayload next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ReadLogPayload payload = next;
            next = null;
            return payload;
        }
    }
}

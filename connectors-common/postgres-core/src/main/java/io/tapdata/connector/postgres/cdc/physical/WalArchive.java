package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.kit.EmptyKit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.DEFAULT_WAL_SEGMENT_SIZE;

/**
 * Resolves PostgreSQL WAL segment names and obtains archived segments.
 *
 * <p>The archive is deliberately independent from PGDATA. A local directory is
 * useful for a mounted PVC/NFS path; a restore command provides the same
 * contract as PostgreSQL's {@code restore_command} for object storage clients.</p>
 */
final class WalArchive {

    interface TimelineResolver {
        int timelineFor(long lsn);
    }

    static final class Range {
        final long startLsn;
        final long endLsn;
        final int timeline;

        Range(long startLsn, long endLsn, int timeline) {
            this.startLsn = startLsn;
            this.endLsn = endLsn;
            this.timeline = timeline;
        }
    }

    private final Path directory;
    private final String restoreCommand;
    private final long segmentSize;
    private final long restoreTimeoutSeconds;

    private WalArchive(Path directory, String restoreCommand, long segmentSize) {
        this.directory = directory;
        this.restoreCommand = restoreCommand;
        this.segmentSize = segmentSize > 0 ? segmentSize : DEFAULT_WAL_SEGMENT_SIZE;
        this.restoreTimeoutSeconds = positiveLong(
                "tapdata.wal.archive.restore.timeout.seconds",
                "TAPDATA_WAL_ARCHIVE_RESTORE_TIMEOUT_SECONDS", 120L);
    }

    static WalArchive from(String configuredDirectory, String configuredRestoreCommand, long segmentSize) {
        String directory = configuredDirectory;
        if (EmptyKit.isBlank(directory)) {
            directory = System.getenv("TAPDATA_WAL_ARCHIVE_DIR");
        }
        String command = configuredRestoreCommand;
        if (EmptyKit.isBlank(command)) {
            command = System.getenv("TAPDATA_WAL_ARCHIVE_RESTORE_COMMAND");
        }
        if (EmptyKit.isBlank(directory) && EmptyKit.isBlank(command)) {
            return null;
        }
        return new WalArchive(EmptyKit.isBlank(directory) ? null : java.nio.file.Paths.get(directory.trim()), command, segmentSize);
    }

    List<Range> ranges(long startLsn, long endLsn, TimelineResolver resolver) {
        List<Range> ranges = new ArrayList<>();
        long cursor = startLsn;
        while (cursor < endLsn) {
            int timeline = resolver.timelineFor(cursor);
            if (timeline <= 0) {
                throw new IllegalStateException("Cannot resolve WAL archive timeline at LSN "
                        + PhysicalWalLogMiner.lsnStr(cursor));
            }
            long low = cursor;
            long high = endLsn;
            while (low < high) {
                long mid = low + ((high - low) >>> 1);
                if (resolver.timelineFor(mid) == timeline) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            long boundary = Math.min(endLsn, low);
            if (boundary <= cursor) {
                boundary = endLsn;
            }
            ranges.add(new Range(cursor, boundary, timeline));
            cursor = boundary;
        }
        ranges.sort(Comparator.comparingLong(r -> r.startLsn));
        return ranges;
    }

    Path obtain(int timeline, long lsn) throws IOException, InterruptedException {
        String fileName = segmentFileName(lsn, timeline, segmentSize);
        if (directory != null) {
            Path direct = directory.resolve(fileName);
            if (Files.isRegularFile(direct)) {
                return direct;
            }
            Path nested = directory.resolve(String.format("%08X", timeline)).resolve(fileName);
            if (Files.isRegularFile(nested)) {
                return nested;
            }
        }
        if (EmptyKit.isBlank(restoreCommand)) {
            return null;
        }
        Path targetDir = directory != null ? directory : Files.createTempDirectory("tapdata-wal-archive-");
        Files.createDirectories(targetDir);
        Path target = targetDir.resolve(fileName);
        String command = restoreCommand.trim()
                .replace("%f", fileName)
                .replace("%p", target.toString())
                .replace("%t", String.valueOf(timeline));
        Process process = new ProcessBuilder("/bin/sh", "-c", command)
                .redirectErrorStream(true)
                .start();
        boolean finished = process.waitFor(restoreTimeoutSeconds, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IOException("WAL archive restore command timed out for " + fileName);
        }
        if (process.exitValue() != 0 || !Files.isRegularFile(target)) {
            return null;
        }
        return target;
    }

    Path obtainHistory(int timeline) throws IOException, InterruptedException {
        String fileName = String.format("%08X.history", timeline);
        if (directory != null) {
            Path direct = directory.resolve(fileName);
            if (Files.isRegularFile(direct)) {
                return direct;
            }
            Path nested = directory.resolve(String.format("%08X", timeline)).resolve(fileName);
            if (Files.isRegularFile(nested)) {
                return nested;
            }
        }
        if (EmptyKit.isBlank(restoreCommand)) {
            return null;
        }
        Path targetDir = directory != null ? directory : Files.createTempDirectory("tapdata-wal-archive-");
        Files.createDirectories(targetDir);
        Path target = targetDir.resolve(fileName);
        String command = restoreCommand.trim()
                .replace("%f", fileName)
                .replace("%p", target.toString())
                .replace("%t", String.valueOf(timeline));
        Process process = new ProcessBuilder("/bin/sh", "-c", command)
                .redirectErrorStream(true)
                .start();
        boolean finished = process.waitFor(restoreTimeoutSeconds, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IOException("WAL archive restore command timed out for " + fileName);
        }
        return process.exitValue() == 0 && Files.isRegularFile(target) ? target : null;
    }

    byte[] readRange(Path segment, long segmentStartLsn, long startLsn, long endLsn) throws IOException {
        long from = Math.max(0L, startLsn - segmentStartLsn);
        long to = Math.min(Files.size(segment), Math.max(from, endLsn - segmentStartLsn));
        if (to <= from) {
            return new byte[0];
        }
        long length = to - from;
        if (length > Integer.MAX_VALUE) {
            throw new IOException("Archived WAL range is too large: " + length);
        }
        byte[] data = new byte[(int) length];
        try (java.io.InputStream in = Files.newInputStream(segment, StandardOpenOption.READ)) {
            long skipped = 0;
            while (skipped < from) {
                long n = in.skip(from - skipped);
                if (n <= 0) {
                    throw new IOException("Cannot seek archived WAL segment " + segment);
                }
                skipped += n;
            }
            int offset = 0;
            while (offset < data.length) {
                int n = in.read(data, offset, data.length - offset);
                if (n < 0) {
                    throw new IOException("Archived WAL segment ended early: " + segment);
                }
                offset += n;
            }
        }
        return data;
    }

    static String segmentFileName(long lsn, int timeline, long segmentSize) {
        long size = segmentSize > 0 ? segmentSize : DEFAULT_WAL_SEGMENT_SIZE;
        long segmentNo = lsn / size;
        long log = segmentNo / 0x100L;
        long segment = segmentNo % 0x100L;
        return String.format("%08X%08X%08X", timeline, log, segment);
    }

    private static long positiveLong(String property, String env, long fallback) {
        String value = System.getProperty(property);
        if (EmptyKit.isBlank(value)) {
            value = System.getenv(env);
        }
        try {
            long parsed = Long.parseLong(value == null ? "" : value.trim());
            return parsed > 0 ? parsed : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }
}

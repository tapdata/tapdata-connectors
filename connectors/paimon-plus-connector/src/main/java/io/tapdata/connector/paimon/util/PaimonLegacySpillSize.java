package io.tapdata.connector.paimon.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/** Bounded metadata-only estimate. Never follows symlinks or opens legacy lock files. */
final class PaimonLegacySpillSize extends java.nio.file.SimpleFileVisitor<Path> {
    final long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(100);
    long bytes;
    int visited;
    boolean incomplete;

    void inspect(Path directory) {
        if (incomplete) { return; }
        try { Files.walkFileTree(directory, this); }
        catch (IOException | RuntimeException failure) { incomplete = true; }
    }

    private java.nio.file.FileVisitResult budget() {
        if (++visited > 10000 || System.nanoTime() - deadline >= 0) {
            incomplete = true;
            return java.nio.file.FileVisitResult.TERMINATE;
        }
        return java.nio.file.FileVisitResult.CONTINUE;
    }

    @Override public java.nio.file.FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attrs) {
        return budget();
    }

    @Override public java.nio.file.FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
        if (attrs.isRegularFile()) { bytes += attrs.size(); }
        return budget();
    }

    @Override public java.nio.file.FileVisitResult visitFileFailed(Path path, IOException failure) {
        incomplete = true;
        return java.nio.file.FileVisitResult.TERMINATE;
    }
}

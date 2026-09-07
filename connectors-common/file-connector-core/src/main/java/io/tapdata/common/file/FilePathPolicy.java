package io.tapdata.common.file;

public final class FilePathPolicy {
    private FilePathPolicy() {
    }

    public static String normalize(String path) {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("path is required");
        }
        String value = path.trim().replace('\\', '/');
        if (value.contains("://") || value.startsWith("/") || value.indexOf('\u0000') >= 0) {
            throw new IllegalArgumentException("path must be relative");
        }
        StringBuilder normalized = new StringBuilder();
        for (String segment : value.split("/")) {
            if (segment.isEmpty() || ".".equals(segment)) {
                continue;
            }
            if ("..".equals(segment)) {
                throw new IllegalArgumentException("path traversal is forbidden");
            }
            if (normalized.length() > 0) normalized.append('/');
            normalized.append(segment);
        }
        if (normalized.length() == 0) {
            throw new IllegalArgumentException("path is empty");
        }
        return normalized.toString();
    }

    public static String resolveRoot(String rootPath, String relativePath) {
        String normalized = normalize(relativePath);
        if (rootPath == null || rootPath.trim().isEmpty() || "/".equals(rootPath.trim())) {
            return "/" + normalized;
        }
        String root = rootPath.trim().replace('\\', '/');
        if (!root.startsWith("/")) root = "/" + root;
        while (root.endsWith("/") && root.length() > 1) root = root.substring(0, root.length() - 1);
        if (root.contains("..") || root.contains("://") || root.indexOf('\u0000') >= 0) {
            throw new IllegalArgumentException("root path is invalid");
        }
        return root + "/" + normalized;
    }
}

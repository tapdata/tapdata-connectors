package io.tapdata.common.file;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FilePathPolicyTest {
    @Test
    void normalizesRelativePathAndRejectsTraversal() {
        assertEquals("orders/a.txt", FilePathPolicy.normalize("orders//a.txt"));
        assertThrows(IllegalArgumentException.class, () -> FilePathPolicy.normalize("../orders/a.txt"));
        assertThrows(IllegalArgumentException.class, () -> FilePathPolicy.normalize("ftp://host/orders/a.txt"));
    }

    @Test
    void joinsRootWithoutAllowingEscape() {
        assertEquals("/incoming/orders/a.txt", FilePathPolicy.resolveRoot("/incoming", "orders/a.txt"));
        assertThrows(IllegalArgumentException.class, () -> FilePathPolicy.resolveRoot("/incoming", "../../etc/passwd"));
    }
}

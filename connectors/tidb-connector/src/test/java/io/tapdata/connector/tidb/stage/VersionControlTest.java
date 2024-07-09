package io.tapdata.connector.tidb.stage;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class VersionControlTest {
    @Test
    void testVersion() {
        String vr = "8.0.0-tidb-v%d.%d.%d";
        int i = 9;
        for (;i>0;i--) {
            int j = 9;
            for (;j>=0;j--) {
                int k = 9;
                for (;k>=0;k--) {
                    VersionControl v = VersionControl.redirect(String.format(vr, i, j, k));
                    Assertions.assertNotNull(v);
                }
            }
        }

        VersionControl v = VersionControl.redirect("xxxxx");
        Assertions.assertNotNull(v);

        VersionControl v1= VersionControl.redirect("8.0.0-tidb-v7.c.7");
        Assertions.assertNotNull(v1);
    }
}
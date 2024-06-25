package io.tapdata.connector.tidb.stage;

public interface VersionControl {

    public static VersionControl redirect(String version) {
        String[] split = version.split("-");
        if (split.length == 3) {
            String tiVersion = split[2];
            int v = VersionControl.versionNumber(tiVersion);
            if (v < 600) {
                return new VersionLower6();
            } else if (v < 800) {
                return new Version6To8();
            } else {
                return new VersionUpper8();
            }
        }
        return new VersionLower6();
    }

    String redirectCDC(String version);

    public static int versionNumber(String version) {
        while (version.contains("v")) {
            version = version.replace("v", "");
        }
        String[] split = version.split("\\.");
        Double v = 0.1D;
        int nv = 0;
        for(int index = split.length -1; index >= 0; index--) {
            v = v * 10;
            String number = split[index];
            Double p = 0D;
            try {
                p = v * Integer.parseInt(number);
            } catch (Exception e) {
                //do nothing
            }
            nv += p.intValue();
        }
        return nv;
    }
}

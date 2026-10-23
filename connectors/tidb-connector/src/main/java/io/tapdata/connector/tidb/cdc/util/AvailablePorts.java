package io.tapdata.connector.tidb.cdc.util;

import java.io.IOException;
import java.net.ServerSocket;

public class AvailablePorts {
    private AvailablePorts() {}
    public static int getAvailable(int referencePort) {
        if (referencePort > 65535 || referencePort < 1024) return referencePort;
        for (int port = referencePort; port <= 65535; port++) {
            if (isPortAvailable(port)) {
                return port;
            }
        }
        return referencePort;
    }

    private static boolean isPortAvailable(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            socket.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}

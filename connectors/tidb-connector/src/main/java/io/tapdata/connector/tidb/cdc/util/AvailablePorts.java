package io.tapdata.connector.tidb.cdc.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

public class AvailablePorts {
    private AvailablePorts() {}
    public static int getAvailable(int referencePort) {
        if (referencePort > 65535 || referencePort < 1024) return referencePort;
        Set<Integer> usedPorts = getUsedPorts();
        for (int port = referencePort; port <= 65535; port++) {
            if (!usedPorts.contains(port) && isPortAvailable(port)) {
                return port;
            }
        }
        return referencePort;
    }

    private static Set<Integer> getUsedPorts() {
        return new HashSet<>(getUsedPortsFromNetStat());
    }

    private static boolean isPortAvailable(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            socket.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static Set<Integer> getUsedPortsFromNetStat() {
        Set<Integer> usedPorts = new HashSet<>();
        try {
            Process process = Runtime.getRuntime().exec("netstat -tuln");
            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("LISTEN")) {
                    String[] tokens = line.split("\\s+");
                    String addressPort = tokens[3]; // Local address and port
                    String portStr = addressPort.substring(addressPort.lastIndexOf(':') + 1);
                    try {
                        int port = Integer.parseInt(portStr);
                        usedPorts.add(port);
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return usedPorts;
    }
}

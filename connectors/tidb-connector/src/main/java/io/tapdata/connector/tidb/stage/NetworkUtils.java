package io.tapdata.connector.tidb.stage;

import io.tapdata.entity.error.CoreException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.List;

public class NetworkUtils {
    public static final int CODE = 2000001;

    private NetworkUtils() {}

    public static boolean check(String ipAddress, int pdServerPort, int tikKVHost) {
        try {
            InetAddress targetAddress = InetAddress.getByName(ipAddress);
            if (!isSameSubnet(targetAddress)) {
                if (!isServerReachable(ipAddress, pdServerPort)) {
                    throw new CoreException(CODE, "Can not connect TiServer {}:{}, network cannot be connected and CDC incremental data cannot be obtained. Please deploy TiDB or configure network routing in the same network segment", ipAddress, pdServerPort);
                }
                if (!isServerReachable(ipAddress, tikKVHost)) {
                    throw new CoreException(CODE, "Can not connect TiKV {}:{}, network cannot be connected and CDC incremental data cannot be obtained. Please deploy TiDB or configure network routing in the same network segment", ipAddress, tikKVHost);
                }
            }
            return true;
        } catch (UnknownHostException e) {
            throw new CoreException(CODE, "Unknown host: {}", ipAddress);
        } catch (SocketException e) {
            throw new CoreException(CODE, "SocketException: {}, ip: {}", e.getMessage(), ipAddress);
        } catch (Exception e) {
            throw new CoreException(CODE, "Check failed, meg: {}", e.getMessage());
        }
    }

    public static boolean isServerReachable(String host, int port) {
        Socket socket = new Socket();
        SocketAddress socketAddress = new InetSocketAddress(host, port);
        try {
            socket.connect(socketAddress, 5000); // 超时时间 5000 毫秒
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                // 忽略关闭异常
            }
        }
    }


    /**
     * 判断给定的 IP 地址是否和当前机器在同一个子网下
     * 
     * @param targetAddress 给定的 IP 地址
     * @return 如果在同一个子网下返回 true，否则返回 false
     * @throws SocketException
     */
    public static boolean isSameSubnet(InetAddress targetAddress) throws SocketException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();

            if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                continue;
            }

            List<InterfaceAddress> interfaceAddresses = networkInterface.getInterfaceAddresses();
            for (InterfaceAddress interfaceAddress : interfaceAddresses) {
                InetAddress localAddress = interfaceAddress.getAddress();
                InetAddress broadcastAddress = interfaceAddress.getBroadcast();

                if (broadcastAddress != null && isSameSubnet(localAddress, targetAddress, interfaceAddress.getNetworkPrefixLength())) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 判断两个 IP 地址是否在同一个子网下
     * 
     * @param localAddress 本地 IP 地址
     * @param targetAddress 目标 IP 地址
     * @param prefixLength 子网前缀长度
     * @return 如果在同一个子网下返回 true，否则返回 false
     */
    public static boolean isSameSubnet(InetAddress localAddress, InetAddress targetAddress, int prefixLength) {
        byte[] localBytes = localAddress.getAddress();
        byte[] targetBytes = targetAddress.getAddress();

        if (localBytes.length != targetBytes.length) {
            return false;
        }

        int fullBytes = prefixLength / 8;
        int remainingBits = prefixLength % 8;

        for (int i = 0; i < fullBytes; i++) {
            if (localBytes[i] != targetBytes[i]) {
                return false;
            }
        }

        if (remainingBits > 0) {
            int mask = (1 << remainingBits) - 1 << (8 - remainingBits);
            if ((localBytes[fullBytes] & mask) != (targetBytes[fullBytes] & mask)) {
                return false;
            }
        }

        return true;
    }
}

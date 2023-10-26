package com.github.shyiko.mysql.binlog.network.protocol.command;

public class CommandUtils {
    public static byte[] xor(byte[] input, byte[] against) {
        byte[] to = new byte[input.length];

        for( int i = 0; i < input.length; i++ ) {
            to[i] = (byte) (input[i] ^ against[i % against.length]);
        }
        return to;
    }
}

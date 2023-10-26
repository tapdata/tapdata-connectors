package com.github.shyiko.mysql.binlog.network.protocol.command;

import java.io.IOException;

public class ByteArrayCommand implements Command {
    private final byte[] command;

    public ByteArrayCommand(byte[] command) {
        this.command = command;
    }
    @Override
    public byte[] toByteArray() throws IOException {
        return command;
    }
}

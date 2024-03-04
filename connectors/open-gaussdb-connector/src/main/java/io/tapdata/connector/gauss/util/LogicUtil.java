package io.tapdata.connector.gauss.util;

import io.tapdata.connector.gauss.entity.IllegalDataLengthException;
import io.tapdata.connector.gauss.enums.CdcConstant;

import java.nio.ByteBuffer;
import java.util.Objects;

public class LogicUtil {
    public static int bytesToInt(byte[] src) {
        return ((Long)byteToNumber(src)).intValue();
    }

    protected static long byteToNumber(byte[] bytes){
        long numberValue = 0;
        if (null == bytes) {
            return numberValue;
        }
        if( bytes.length == 0) {
            return numberValue;
        }
        for (byte bitValue : bytes) {
            numberValue <<= 8;
            numberValue |= (bitValue & 0xff);
        }
        return numberValue;
    }

    public static long byteToLong(byte[] bs){
        return byteToNumber(bs);
    }


    public static byte[] read(ByteBuffer buffer, int readByteSize) {
        byte[] result = new byte[readByteSize];
        int index = 0;
        while (buffer.hasRemaining() && index < readByteSize) {
            result[index] = buffer.get();
            index++;
        }
        return result;
    }

    public static byte[] read(ByteBuffer buffer, int lengthByteSize, int bitOffset) {
        byte[] lengthBytes = read(buffer, lengthByteSize);
        int readSize = bytesToInt(lengthBytes);
        if (readSize < 0) {
            throw new IllegalDataLengthException("Illegal data length: " + readSize );
        }
        return read(buffer, readSize);
    }

    public static byte[] readValue(ByteBuffer buffer, int lengthByteSize, int bitOffset) {
        byte[] lengthBytes = read(buffer, lengthByteSize);
        int readSize = bytesToInt(lengthBytes);
        if (Objects.equals(CdcConstant.BYTES_VALUE_OF_NULL, readSize)) return null;
        if (readSize < 0) {
            throw new IllegalDataLengthException("Illegal data length: " + readSize );
        }
        if (Objects.equals(CdcConstant.BYTES_VALUE_OF_EMPTY_CHAR, readSize)) return "".getBytes();
        return read(buffer, readSize);
    }
}

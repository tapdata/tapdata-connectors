package io.tapdata.kit;

import java.math.BigInteger;

public class NumberKit {

    /**
     * convert bytes to long for debezium records
     *
     * @param bs byte array
     * @return long
     */
    public static long bytes2long(byte[] bs) {
        long l = 0;
        int size = bs.length;
        for (int i = 0; i < size; i++) {
            l |= (bs[i] & 0xffL) << (size - i - 1) * 8;
        }
        return l;
    }

    public static BigInteger debeziumBytes2long(byte[] bs) {
        if(bs[0] >= 0) {
            return new BigInteger(bs);
        } else {
            return new BigInteger(flipBytes(bs)).not();
        }
    }

    public static byte[] flipBytes(byte[] bytes) {
        byte[] flippedBytes = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            flippedBytes[i] = (byte) ~bytes[i];  // 对每个字节进行按位取反
        }
        return flippedBytes;
    }

    public static void main(String[] args) {
        byte[] bs = new byte[]{0, -16, -72, -23};
        System.out.println(debeziumBytes2long(bs));
        byte[] bs2 = new byte[]{-16, -72, -23};
        System.out.println(debeziumBytes2long(bs2));
    }
}

package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.connector.gauss.enums.CdcConstant;

import java.nio.ByteBuffer;

public class LogicUtil {
    public static void main(String[] args) {
        byte[] b= new byte[]{-1,-1,-1,-1};
        System.out.println(bytesToInt(b, 32));
    }
    public static int bytesToInt(byte[] src, int offset) {
        int ans = 0;
        for(int i = 0; i < src.length; i++){
            ans <<= offset;
            ans |= (src[src.length - i - 1]);
            //ans |= (src[src.length - i - 1] & 0xff);
        }
        return ans;
    }

    public static long bytesToLong(byte[] bs){
        int bytes = bs.length;
        if(bytes > 1) {
            if((bytes % 2) != 0 || bytes > 8) {
                return 0;
            }}
        switch(bytes) {
            case 1:
                return (long)((bs[0] & 0xff));
            case 2:
                return (long)((bs[0] & 0xff) <<8 | (bs[1] & 0xff));
            case 4:
                return (long)((bs[0] & 0xffL) <<24 | (bs[1] & 0xffL) << 16 | (bs[2] & 0xffL) <<8 | (bs[3] & 0xffL));
            case 8:
                return (long)((bs[0] & 0xffL) <<56 | (bs[1] & 0xffL) << 48 | (bs[2] & 0xffL) <<40 | (bs[3] & 0xffL)<<32 |
                        (bs[4] & 0xffL) <<24 | (bs[5] & 0xffL) << 16 | (bs[6] & 0xffL) <<8 | (bs[7] & 0xffL));
            case 0:
            default:
                return 0;
        }
        //return 0;
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
        int readSize = bytesToInt(lengthBytes, bitOffset);
        return read(buffer, readSize);
    }

    public static byte[] readValue(ByteBuffer buffer, int lengthByteSize, int bitOffset) {
        byte[] lengthBytes = read(buffer, lengthByteSize);
        int readSize = bytesToInt(lengthBytes, bitOffset);
        if (CdcConstant.BYTES_VALUE_OF_NULL == readSize) return null;
        if (CdcConstant.BYTES_VALUE_OF_EMPTY_CHAR == readSize) return "".getBytes();
        return read(buffer, readSize);
    }
}

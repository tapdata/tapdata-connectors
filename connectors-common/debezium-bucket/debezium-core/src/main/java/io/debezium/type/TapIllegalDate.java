package io.debezium.type;

import java.io.*;

public class TapIllegalDate implements Serializable{
    private String originDate;
    private Class originDateType;

    public Class getOriginDateType() {
        return originDateType;
    }

    public void setOriginDateType(Class originDateType) {
        this.originDateType = originDateType;
    }

    public String getOriginDate() {
        return originDate;
    }

    public void setOriginDate(String originDate) {
        this.originDate = originDate;
    }

    public byte[] illegalDateToByte(TapIllegalDate illegalDate) throws IOException {
        byte[] bytes;
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(bo);
            oo.writeObject(illegalDate);

            bytes = bo.toByteArray();

            bo.close();
            oo.close();
        } catch (Exception e) {
            throw e;
        }
        return (bytes);
    }
    public static Object byteToIllegalDate(byte[] bytes) throws IOException, ClassNotFoundException {
        java.lang.Object obj;
        try {
            ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
            ObjectInputStream oi = new ObjectInputStream(bi);

            obj = oi.readObject();

            bi.close();
            oi.close();
        }
        catch(Exception e) {
            throw e;
        }
        return obj;
    }
}

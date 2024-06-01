package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.entity.error.CoreException;

import java.math.BigDecimal;

/**
 * @author GavinXiao
 * @description DefaultConvert create by Gavin
 * @create 2023/7/15 18:36
 **/
public class DefaultConvert implements TiDBDataTypeConvert {
    public static final String TAG = TiDBDataTypeConvert.class.getSimpleName();
    public static final int CONVERT_ERROR_CODE = 362430;

    @Override
    public Object convert(Object fromValue, final String sybaseType, final int typeNum) {
        if (null == fromValue || null == sybaseType) return null;
        if (fromValue instanceof String && "NULL".equals(fromValue)) return null;
        BigDecimal bigDecimal = null;

        String type = sybaseType.toUpperCase();
        try {
            switch (typeNum) {
                case TableTypeEntity.Type.CHAR:
                case TableTypeEntity.Type.TEXT:
                    return TiDBDataTypeConvert.objToString(fromValue);
                case TableTypeEntity.Type.DATE:
                    return TiDBDataTypeConvert.objToDateTime(fromValue, "yyyy-MM-dd", "DATE");
                case TableTypeEntity.Type.TIME:
                    return TiDBDataTypeConvert.objToDateTime(fromValue, "HH:mm:ss", "TIME");
                case TableTypeEntity.Type.BIG_TIME:
                    return TiDBDataTypeConvert.objToDateTime(fromValue, "HH:mm:ss.SSS", "BIGTIME");
                case TableTypeEntity.Type.DATETIME:
                case TableTypeEntity.Type.SMALL_DATETIME:
                    return TiDBDataTypeConvert.objToTimestamp(fromValue, type);
                case TableTypeEntity.Type.INT:
                    bigDecimal = TiDBDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.intValue();
                case TableTypeEntity.Type.SMALLINT:
                    bigDecimal = TiDBDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.byteValue();
                case TableTypeEntity.Type.BIGINT:
                    bigDecimal = TiDBDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.longValue();
                case TableTypeEntity.Type.DOUBLE:
                    bigDecimal = TiDBDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.doubleValue();
                case TableTypeEntity.Type.BIT:
                case TableTypeEntity.Type.FLOAT:
                    bigDecimal = TiDBDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.floatValue();
                case TableTypeEntity.Type.IMAGE:
                case TableTypeEntity.Type.BINARY:
                    return TiDBDataTypeConvert.objToBinary(fromValue);
                case TableTypeEntity.Type.DECIMAL:
                    return TiDBDataTypeConvert.objToNumber(fromValue);
                case TableTypeEntity.Type.NUMERIC:
                    return TiDBDataTypeConvert.objToNumber(fromValue);
                default:
                    throw new CoreException(CONVERT_ERROR_CODE, "Found a type that cannot be processed when cdc: {}", sybaseType);

            }
        } catch (Exception e) {
            if (e instanceof CoreException && ((CoreException) e).getCode() == CONVERT_ERROR_CODE) {
                throw (CoreException) e;
            }
            throw new CoreException("Can not convert value {} to {} value, msg: {}", fromValue, type, e.getMessage());
        }
    }

    private String dateTimeFormat(String datetimeType, final int defaultCount) {
        String formatStart = "yyyy-MM-dd HH:mm:ss";
        if (null == datetimeType || "".equals(datetimeType)) return formatStart;
        int sCount = defaultCount;
        if (datetimeType.matches(".*\\(\\d*\\).*")) {
            int index = datetimeType.lastIndexOf('(');
            int lIndex = datetimeType.lastIndexOf(')');
            if (index > 0 && lIndex > index) {
                try {
                    sCount = Integer.parseInt(datetimeType.substring(index, lIndex));
                } catch (Exception e) {

                }
            }
        }
        if (sCount < 0 && defaultCount > 0) {
            sCount = defaultCount;
        }
        if (sCount > 0) {
            StringBuilder format = new StringBuilder(formatStart);
            format.append(".");
            for (int i = 0; i < sCount; i++) {
                format.append("S");
            }
            return format.toString();
        }
        return formatStart;
    }
}

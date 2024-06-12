package io.tapdata.connector.tidb.cdc.process.ddl.convert;

public class CharConvert implements Convert {
    int columnPrecision;

    public CharConvert(String columnPrecision) {
       this.columnPrecision = Integer.parseInt(columnPrecision);
    }

    public CharConvert(int columnPrecision) {
        if (columnPrecision < 1) columnPrecision = 1;
        this.columnPrecision = columnPrecision;
    }

    @Override
    public Object convert(Object fromValue) {
        if (null == fromValue) return null;
        String value = String.valueOf(fromValue);
        return value.length() > columnPrecision ? value.substring(0, columnPrecision) : value;
    }
}

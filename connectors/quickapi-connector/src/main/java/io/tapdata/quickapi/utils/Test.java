package io.tapdata.quickapi.utils;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/5 15:32 Create
 * @description
 */
public class Test {
    public static void main(String[] args) {
        System.out.println("string: " + "string".matches(SchemaParser.TYPE_MATCH));
        System.out.println("string1: " + "string1".matches(SchemaParser.TYPE_MATCH));
        System.out.println("integer: " + "integer".matches(SchemaParser.TYPE_MATCH));
        System.out.println("intger: " + "intger".matches(SchemaParser.TYPE_MATCH));
        System.out.println("long: " + "long".matches(SchemaParser.TYPE_MATCH));
        System.out.println("Long: " + "Long".matches(SchemaParser.TYPE_MATCH));
        System.out.println("double: " + "double".matches(SchemaParser.TYPE_MATCH));
        System.out.println("date: " + "date".matches(SchemaParser.TYPE_MATCH));
        System.out.println("array: " + "array".matches(SchemaParser.TYPE_MATCH));
        System.out.println("boolean: " + "boolean".matches(SchemaParser.TYPE_MATCH));
        System.out.println("timestamp: " + "timestamp".matches(SchemaParser.TYPE_MATCH));
        System.out.println("timestamp(0): " + "timestamp".matches(SchemaParser.TYPE_MATCH));
        System.out.println("timestamp(1): " + "timestamp".matches(SchemaParser.TYPE_MATCH));
        System.out.println("datetime: " + "datetime".matches(SchemaParser.TYPE_MATCH));
        System.out.println("datetime(3): " + "datetime".matches(SchemaParser.TYPE_MATCH));
        System.out.println("datetime(11): " + "datetime".matches(SchemaParser.TYPE_MATCH));
        System.out.println("time: " + "time".matches(SchemaParser.TYPE_MATCH));
        System.out.println("time(1): " + "time(1)".matches(SchemaParser.TYPE_MATCH));
        System.out.println("time(22): " + "time(22)".matches(SchemaParser.TYPE_MATCH));
    }
}

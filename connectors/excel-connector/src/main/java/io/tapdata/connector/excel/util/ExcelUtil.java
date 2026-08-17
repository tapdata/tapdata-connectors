package io.tapdata.connector.excel.util;

import io.tapdata.kit.EmptyKit;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class ExcelUtil {

    //3,5~9,12
    public static List<Integer> getSheetNumber(String reg) {
        if (EmptyKit.isBlank(reg)) {
            return Collections.emptyList();
        }
        Set<Integer> set = new HashSet<>();
        String[] arr = reg.split(",");
        Arrays.stream(arr).forEach(v -> {
            if (v.contains("~")) {
                for (int i = Integer.parseInt(v.substring(0, v.indexOf("~"))); i <= Integer.parseInt(v.substring(v.indexOf("~") + 1)); i++) {
                    set.add(i);
                }
            } else {
                set.add(Integer.parseInt(v));
            }
        });
        return set.stream().sorted(Comparator.naturalOrder()).collect(Collectors.toList());
    }

    public static List<Integer> getAllSheetNumber(int sheetNumber) {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= sheetNumber; i++) {
            list.add(i);
        }
        return list;
    }

    public static int getColumnNumber(String letter) {
        int res = 0;
        char[] arr = letter.toCharArray();
        for (char c : arr) {
            res = 26 * res + (int) c + 1 - (int) 'A';
        }
        return res;
    }

    public static Map<CellRangeAddress, Cell> getMergedDataMap(Sheet sheet) {
        return sheet.getMergedRegions().stream().collect(Collectors.toMap(v -> v, v -> sheet.getRow(v.getFirstRow()).getCell(v.getFirstColumn())));
    }

    public static Object getMergedCellValue(List<CellRangeAddress> mergedList, Map<CellRangeAddress, Cell> mergedDataMap, Cell cell, FormulaEvaluator formulaEvaluator) {
        if (EmptyKit.isNull(cell)) {
            return null;
        }
        if (EmptyKit.isEmpty(mergedList)) {
            return getCellValue(cell, formulaEvaluator);
        }
        for (CellRangeAddress merge : mergedList) {
            if (merge.isInRange(cell)) {
                return getCellValue(mergedDataMap.get(merge), formulaEvaluator);
            }
        }
        return getCellValue(cell, formulaEvaluator);
    }

    public static String getMergedCellDisplayValue(List<CellRangeAddress> mergedList, Map<CellRangeAddress, Cell> mergedDataMap, Cell cell, FormulaEvaluator formulaEvaluator, DataFormatter dataFormatter) {
        if (EmptyKit.isNull(cell)) {
            return null;
        }
        if (EmptyKit.isEmpty(mergedList)) {
            return getCellDisplayValue(cell, formulaEvaluator, dataFormatter);
        }
        for (CellRangeAddress merge : mergedList) {
            if (merge.isInRange(cell)) {
                return getCellDisplayValue(mergedDataMap.get(merge), formulaEvaluator, dataFormatter);
            }
        }
        return getCellDisplayValue(cell, formulaEvaluator, dataFormatter);
    }

    public static String getCellDisplayValue(Cell cell, FormulaEvaluator formulaEvaluator, DataFormatter dataFormatter) {
        if (EmptyKit.isNull(cell)) {
            return null;
        }
        DataFormatter formatter = dataFormatter == null ? new DataFormatter() : dataFormatter;
        try {
            return formatter.formatCellValue(cell, formulaEvaluator);
        } catch (Exception e) {
            if (CellType.FORMULA.equals(cell.getCellType())) {
                return "=" + cell.getCellFormula();
            }
            return String.valueOf(getCellValue(cell, formulaEvaluator));
        }
    }

    static Object parseNumberValue(Cell cell) {
        return parseNumberValue(cell.getNumericCellValue(), cell.getCellStyle());
    }

    static Object parseNumberValue(double val, CellStyle cellStyle) {
        BigDecimal bigDecimal = BigDecimal.valueOf(val);
        if (hasDecimalPlaceholders(cellStyle)) {
            return bigDecimal.doubleValue();
        }
        return bigDecimal.stripTrailingZeros().longValue();
    }

    private static boolean hasDecimalPlaceholders(CellStyle cellStyle) {
        if (cellStyle == null || EmptyKit.isBlank(cellStyle.getDataFormatString())) {
            return false;
        }
        String format = cellStyle.getDataFormatString();
        if ("General".equalsIgnoreCase(format)) {
            return false;
        }
        boolean inQuote = false;
        boolean escaped = false;
        boolean afterDecimalPoint = false;
        for (int i = 0; i < format.length(); i++) {
            char c = format.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inQuote = !inQuote;
                continue;
            }
            if (inQuote) {
                continue;
            }
            if (c == ';') {
                afterDecimalPoint = false;
                continue;
            }
            if (c == '.') {
                afterDecimalPoint = true;
                continue;
            }
            if (afterDecimalPoint && (c == '0' || c == '#' || c == '?')) {
                return true;
            }
        }
        return false;
    }

    public static Object getCellValue(Cell cell, FormulaEvaluator formulaEvaluator) {
        if (EmptyKit.isNull(cell)) {
            return null;
        }
        switch (cell.getCellType()) {
            case BOOLEAN:
                return cell.getBooleanCellValue();

            case STRING:
                return cell.getRichStringCellValue().getString();

            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell) || cell.getCellStyle().getDataFormat() == 58) {
                    return Instant.ofEpochMilli((cell.getDateCellValue()).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
                } else {
                    return parseNumberValue(cell);
                }
            case FORMULA:
                if (formulaEvaluator != null) {
                    CellType cellType = null;
                    // 当以等号开头，excel中会认为是一个函数单元格
                    // 当函数无法执行、语法出错、本身值以等号开头，都会导致下面这一行报错
                    // 当前处理方法，用try避免报错，如果异常，代表函数无法执行，则直接取该单元格的字符串值
                    try {
                        cellType = formulaEvaluator.evaluateFormulaCell(cell);
                    } catch (Exception e) {
                    }
                    if (cellType == null) {
                        return "=" + cell.getCellFormula();
                    }
                    switch (cellType) {
                        case BOOLEAN:
                            return cell.getBooleanCellValue();

                        case STRING:
                            return cell.getRichStringCellValue().getString();

                        case NUMERIC:
                            if (DateUtil.isCellDateFormatted(cell)) {
                                return Instant.ofEpochMilli((cell.getDateCellValue()).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
                            } else {
                                return parseNumberValue(cell);
                            }
                        case FORMULA:
                            return cell.getCellFormula();
                        case BLANK:
                        case ERROR:
                        case _NONE:
                        default:
                            return "";
                    }
                }
                return cell.getCellFormula();
            case BLANK:
            case ERROR:
            case _NONE:
            default:
                return "";
        }
    }

    public static void main(String[] args) {
        System.out.println(getColumnNumber("BB"));
        System.out.println(getSheetNumber("7~10,11~13,17,2,5"));
    }
}

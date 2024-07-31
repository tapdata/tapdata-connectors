package com.opencsv;

import java.util.Locale;

public class CustomCsvParser extends CSVParser {
    public CustomCsvParser(char separator) {
        super(separator, DEFAULT_QUOTE_CHARACTER,
                DEFAULT_ESCAPE_CHARACTER, DEFAULT_STRICT_QUOTES,
                DEFAULT_IGNORE_LEADING_WHITESPACE,
                DEFAULT_IGNORE_QUOTATIONS,
                DEFAULT_NULL_FIELD_INDICATOR, Locale.getDefault());
    }
}

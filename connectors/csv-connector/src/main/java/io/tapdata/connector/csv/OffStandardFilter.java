package io.tapdata.connector.csv;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OffStandardFilter {


    private final String lineExpression;

    public OffStandardFilter(String lineExpression) {
        this.lineExpression = lineExpression;
    }

    public String[] filter(String lineStr) {
        try {
            List<String> list = new ArrayList<>();
            Pattern pattern = Pattern.compile(lineExpression);
            Matcher matcher = pattern.matcher(lineStr);
            while (matcher.find()) {
                list.add(matcher.group(1));
            }
            return list.toArray(new String[0]);
        } catch (Exception e) {
            return null;
        }
    }


}

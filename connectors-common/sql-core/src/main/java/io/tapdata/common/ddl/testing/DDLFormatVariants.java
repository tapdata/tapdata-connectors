package io.tapdata.common.ddl.testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Builds deterministic lexical variants of a DDL statement for parser tests.
 * Quoted identifiers, string literals and comments are kept byte-for-byte so
 * only SQL formatting and unquoted keyword/identifier casing are exercised.
 */
public final class DDLFormatVariants {
    private static final List<String> WHITESPACE = Arrays.asList(" ", "  ", "\t", "\n", "\r\n", " \n\t ");

    private DDLFormatVariants() {
    }

    /**
     * Returns the original statement and a bounded matrix of case, whitespace,
     * and inline-comment variations. Output order is stable and duplicates are
     * removed.
     */
    public static List<String> variants(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return new ArrayList<>();
        }

        Set<String> variants = new LinkedHashSet<>();
        variants.add(sql);
        variants.add(mapUnquoted(sql, 0));
        variants.add(mapUnquoted(sql, 1));
        variants.add(mapUnquoted(sql, 2));

        for (String separator : WHITESPACE) {
            variants.add(replaceWhitespace(sql, separator));
            variants.add(replaceWhitespace(mapUnquoted(sql, 2), separator));
        }

        // A block comment in a whitespace position is a useful lexer/parser
        // regression case while remaining valid in common SQL dialects.
        for (String comment : Arrays.asList("/* ddl format */ ", "-- ddl format\n")) {
            String commented = insertCommentAtFirstClauseGap(sql, comment);
            if (commented != null) {
                variants.add(commented);
                variants.add(mapUnquoted(commented, 2));
            }
        }
        return new ArrayList<>(variants);
    }

    private static String replaceWhitespace(String sql, String replacement) {
        StringBuilder out = new StringBuilder(sql.length() + 16);
        char quote = 0;
        boolean lineComment = false;
        boolean blockComment = false;
        boolean pendingWhitespace = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : 0;
            if (lineComment) {
                out.append(c);
                if (c == '\n' || c == '\r') lineComment = false;
                continue;
            }
            if (blockComment) {
                out.append(c);
                if (c == '*' && next == '/') {
                    out.append(next);
                    i++;
                    blockComment = false;
                }
                continue;
            }
            if (quote != 0) {
                out.append(c);
                if (c == quote) {
                    if (next == quote) {
                        out.append(next);
                        i++;
                    } else {
                        quote = 0;
                    }
                }
                continue;
            }
            if ((c == '-' && next == '-') || (c == '/' && next == '*')) {
                if (pendingWhitespace) out.append(replacement);
                pendingWhitespace = false;
                out.append(c).append(next);
                i++;
                if (c == '-') lineComment = true; else blockComment = true;
            } else if (c == '\'' || c == '"' || c == '`') {
                if (pendingWhitespace) out.append(replacement);
                pendingWhitespace = false;
                quote = c;
                out.append(c);
            } else if (Character.isWhitespace(c)) {
                pendingWhitespace = true;
            } else {
                if (pendingWhitespace) out.append(replacement);
                pendingWhitespace = false;
                out.append(c);
            }
        }
        return out.toString();
    }

    /** mode 0 = upper, 1 = lower, 2 = alternating; preserve quoted/comment text. */
    private static String mapUnquoted(String sql, int mode) {
        StringBuilder out = new StringBuilder(sql.length());
        char quote = 0;
        boolean lineComment = false;
        boolean blockComment = false;
        int letter = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : 0;
            if (lineComment) {
                out.append(c);
                if (c == '\n' || c == '\r') lineComment = false;
            } else if (blockComment) {
                out.append(c);
                if (c == '*' && next == '/') {
                    out.append(next);
                    i++;
                    blockComment = false;
                }
            } else if (quote != 0) {
                out.append(c);
                if (c == quote) {
                    if (next == quote) {
                        out.append(next);
                        i++;
                    } else {
                        quote = 0;
                    }
                }
            } else if ((c == '-' && next == '-') || (c == '/' && next == '*')) {
                out.append(c).append(next);
                i++;
                if (c == '-') lineComment = true; else blockComment = true;
            } else if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                out.append(c);
            } else if (Character.isLetter(c)) {
                if (mode == 0) out.append(Character.toUpperCase(c));
                else if (mode == 1) out.append(Character.toLowerCase(c));
                else out.append((letter++ % 2 == 0) ? Character.toUpperCase(c) : Character.toLowerCase(c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static String insertCommentAtFirstClauseGap(String sql, String comment) {
        char quote = 0;
        boolean lineComment = false;
        boolean blockComment = false;
        boolean bracketIdentifier = false;
        boolean afterAlter = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : 0;
            if (lineComment) {
                if (c == '\n' || c == '\r') lineComment = false;
                continue;
            }
            if (blockComment) {
                if (c == '*' && next == '/') {
                    blockComment = false;
                    i++;
                }
                continue;
            }
            if (bracketIdentifier) {
                if (c == ']') {
                    if (next == ']') i++;
                    else bracketIdentifier = false;
                }
                continue;
            }
            if (quote != 0) {
                if (c == quote) {
                    if (next == quote) i++;
                    else quote = 0;
                }
                continue;
            }
            if ((c == '-' && next == '-') || (c == '/' && next == '*')) {
                if (c == '-') lineComment = true;
                else blockComment = true;
                i++;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                continue;
            }
            if (c == '[') {
                bracketIdentifier = true;
                continue;
            }
            if (!Character.isLetter(c) && c != '_') continue;

            int start = i;
            do {
                i++;
                if (i >= sql.length()) break;
                c = sql.charAt(i);
            } while (Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#');
            String word = sql.substring(start, i).toLowerCase(Locale.ROOT);
            if (word.equals("alter")) {
                afterAlter = true;
            } else if (afterAlter && (word.equals("add") || word.equals("modify")
                    || word.equals("drop") || word.equals("rename"))) {
                return sql.substring(0, start) + comment + sql.substring(start);
            }
            i--;
        }
        return null;
    }
}

package io.tapdata.connector.postgres.cdc;


import io.tapdata.common.sqlparser.CDCSQLParser;
import io.tapdata.common.sqlparser.Operate;
import io.tapdata.common.sqlparser.ResultDO;
import io.tapdata.common.sqlparser.SQLReader;

import java.util.List;

/**
 * Oracle增量SQL解析
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/8/24 下午4:15
 * </pre>
 */
public class PostgresCDCSQLParser extends CDCSQLParser {

    public ResultDO from(String sql, Boolean undo) {
        SQLReader sr = SQLReader.build(sql);
        switch (sr.current()) {
            case 'i':
            case 'I':
                if (sr.equalsIgnoreCaseAndMove("insert") && sr.nextAndSkip(isSkip) && sr.equalsIgnoreCaseAndMove("into") && sr.nextAndSkip(isSkip)) {
                    return insertBuild(sr);
                }
                break;
            case 'd':
            case 'D':
                if (sr.equalsIgnoreCaseAndMove("delete") && sr.nextAndSkip(isSkip)) {
                    return deleteBuild(sr);
                }
                break;
            case 'u':
            case 'U':
                if (sr.equalsIgnoreCaseAndMove("update") && sr.nextAndSkip(isSkip)) {
                    return undo ? undoUpdateBuild(sr) : updateBuild(sr);
                }
                break;
            default:
                break;
        }
        throw sr.ex("SQL must start with 'INSERT' or 'DELETE' or 'UPDATE'");
    }

    protected ResultDO updateBuild(SQLReader sr) {
        String tmp;
        ResultDO result = new ResultDO(Operate.UPDATE);

        setTableName(sr, result);
        if (!sr.equalsIgnoreCaseAndMove("set") || !sr.nextAndSkip(isSkip)) {
            throw sr.ex("Not found 'set' after table name");
        }
        if (sr.getData().length() > sr.position() + 6 && sr.getData().substring(sr.position(), sr.position() + 6).equalsIgnoreCase("where ")) {
            return null;
        }
        // load set
        while (true) {
            tmp = loadName(sr, CANNOT_FIND_COLUMN_NAME);
            sr.nextAndSkip(isSkip);
            result.putData(tmp, loadConditionValue(sr));
            sr.nextAndSkip(isSkip);
            if (sr.current(',')) {
                sr.nextAndSkip(isSkip);
                continue;
            }
            break;
        }

        setWhere(sr, result);
        return result;
    }

    protected ResultDO undoUpdateBuild(SQLReader sr) {
        ResultDO result = new ResultDO(Operate.UPDATE);
        setTableName(sr, result);
        if (!sr.equalsIgnoreCaseAndMove("set") || !sr.nextAndSkip(isSkip)) {
            throw sr.ex("Not found 'set' after table name");
        }
        if (sr.getData().length() > sr.position() + 6 && sr.getData().substring(sr.position(), sr.position() + 6).equalsIgnoreCase("where ")) {
            return null;
        }
        // load set
        while (true) {
            loadName(sr, "Can't found column name");
            sr.nextAndSkip(isSkip);
            loadConditionValue(sr);
            sr.nextAndSkip(isSkip);
            if (sr.current(',')) {
                sr.nextAndSkip(isSkip);
                continue;
            }
            break;
        }
        setWhere(sr, result);
        return result;
    }

    protected ResultDO insertBuild(SQLReader sr) {
        ResultDO result = new ResultDO(Operate.INSERT);
        setTableName(sr, result);
        List<String> names = loadInQuotaNames(sr);
        if ((sr.equalsIgnoreCaseAndMove("values") || sr.equalsIgnoreCaseAndMove("value")) && (sr.current('(') || sr.nextAndSkip(isSkip))) {
            sr.currentCheck('(', "Can't found '(' after values");
            sr.nextAndSkip(isSkip);
            result.putData(names.get(0), loadValue(sr));
            sr.nextAndSkip(isSkip);
            for (int i = 1, len = names.size(); i < len; i++) {
                sr.currentCheck(',', "Can't found more values");
                sr.nextAndSkip(isSkip);
                result.putData(names.get(i), loadValue(sr));
                sr.nextAndSkip(isSkip);
            }
            sr.currentCheck(')', "Can't found ')' end values");
        } else {
            throw sr.ex("Can't found 'values' or value");
        }
        return result;
    }

    @Override
    protected Object loadValue(SQLReader sr) {
        if (sr.current(valueQuote)) {
            return sr.loadInQuote(50);
        } else {
            String tmp = loadName(sr, "Can't found function name");
            if ("Error".equals(tmp)) {// 解决Oracle源表日期类型有异常值导致的logminer返回Error Translating
                // Error Translating没有quote包裹, 所以position后移一位
                sr.moveTo(sr.position() + 1);
                if (sr.equalsAndMove(" Translating")) {
                    // 前一步后移一位的, 要回退一位
                    sr.moveTo(sr.position() - 1);
                    return "Error Translating";
                } else {
                    // 前一步后移一位的, 要回退一位
                    sr.moveTo(sr.position() - 1);
                }
            } else {
                // 如果不是Error Translating问题, 需要position回退tmp的长度
                sr.moveTo(sr.position() - tmp.length() + 1);
            }
        }
        if (sr.current(valueQuote)) {
            return sr.loadInQuote(50, escape);
        } else if (isNumber.check(sr.current())) {
            return sr.loadIn(isBinary, "Can't found number value");
        } else if (judgeBit(sr)) {
            return loadValue(sr);
        } else {
            String tmp = loadName(sr, "Can't found function name");
            if ("true".equalsIgnoreCase(tmp)) {
                return true;
            } else if ("false".equalsIgnoreCase(tmp)) {
                return false;
            } else if ("null".equalsIgnoreCase(tmp)) {
                return null;
            } else if (sr.nextAndSkip(isSkip) && sr.current('(')) {
                return tmp + sr.loadInQuoteMulti(50, ')');
            }
            throw sr.ex("Value error '" + tmp + "'");
        }
    }

    public boolean judgeBit(SQLReader sr) {
        if (sr.length() >= sr.position() + 3) {
            if(sr.getData().charAt(sr.position()) == 'B' && sr.getData().charAt(sr.position() + 1) == '\'') {
                sr.moveTo(sr.position() + 1);
                return true;
            }
        }
        return false;
    }

    protected void setWhere(SQLReader sr, ResultDO result) {
        // load where
        if (!sr.equalsIgnoreCaseAndMove("where") || !sr.nextAndSkip(isSkip)) {
            throw sr.ex("Not found 'where'");
        }
        String tmp = loadName(sr, "Can't found column name");
        sr.nextAndSkip(isSkip);
        result.putIfAbsent(tmp, loadConditionValue(sr));

        // set condition
        while (sr.nextAndSkip(isSkip)) {
            if (sr.current(';')) {
                break;
            } else if (sr.equalsIgnoreCaseAndMove("and")) {
                sr.nextAndSkip(isSkip);
                tmp = loadName(sr, "Can't found column name");
                sr.nextAndSkip(isSkip);

                if (result.getData().containsKey(tmp)) {
                    loadConditionValue(sr);
                    continue;
                }
                result.putIfAbsent(tmp, loadConditionValue(sr));
            } else {
                throw sr.ex("Condition not start 'and'");
            }
        }
    }

}
